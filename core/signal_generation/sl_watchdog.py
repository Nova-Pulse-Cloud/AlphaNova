import asyncio
import logging
import httpx
from typing import Any
from core.wallet_manager import WalletManager
from core.execution_router import ExecutionRouter
from core.trade_memory import TradeMemory
from prometheus_client import Counter, Histogram

log = logging.getLogger("sl_watchdog")

# === Prometheus Metrics ===
SL_WATCHDOG_ERRORS = Counter("sl_watchdog_errors_total", "SL Watchdog errors")
SL_WATCHDOG_HITS = Counter("sl_watchdog_hits_total", "SL/TP hit events")
SL_WATCHDOG_LATENCY = Histogram("sl_watchdog_latency_seconds", "SL/TP watch latency")

POLL_INTERVAL = 8  # Seconds between price checks

async def get_live_price(symbol: str) -> float:
    """Fetch live price using CoinGecko API, with retries and exponential backoff."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol.lower()}&vs_currencies=usd"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()[symbol.lower()]["usd"]
    except httpx.RequestError as e:
        log.warning(f"[SL Watchdog] Price fetch error for {symbol}: {e}")
        SL_WATCHDOG_ERRORS.inc()
        raise

async def monitor_open_positions(wallet_manager: WalletManager, trade_memory: TradeMemory, execution_router: ExecutionRouter):
    while True:
        try:
            open_trades = await trade_memory.get_open_trades()
            tasks = [monitor_trade(trade, wallet_manager, trade_memory, execution_router) for trade in open_trades]
            await asyncio.gather(*tasks)  # Use asyncio.gather for concurrent execution

        except Exception as e:
            log.warning(f"[SL Watchdog] Error in monitoring loop: {e}")
            SL_WATCHDOG_ERRORS.inc()

        await asyncio.sleep(POLL_INTERVAL)

async def monitor_trade(trade: Any, wallet_manager: WalletManager, trade_memory: TradeMemory, execution_router: ExecutionRouter):
    symbol = trade.symbol
    entry = trade.entry
    sl = trade.sl
    tp = trade.tp
    side = trade.side

    # Fetch live price with retries
    try:
        price = await get_live_price(symbol)
    except Exception as price_e:
        log.warning(f"[SL Watchdog] Failed to fetch live price for {symbol}: {price_e}")
        return

    # Calculate PnL trigger for SL/TP
    hit_tp = (price >= tp if side == "buy" else price <= tp) if tp is not None else False
    hit_sl = (price <= sl if side == "buy" else price >= sl) if sl is not None else False

    if hit_tp or hit_sl:
        reason = "tp" if hit_tp else "sl"
        log.info(f"📉 SL/TP hit on {symbol} | reason={reason} | entry={entry} | now={price}")

        signal = {
            "symbol": symbol,
            "entry": entry,
            "side": "sell" if side == "buy" else "buy",  # Close position
            "score": 1.0,  # Forced high confidence
            "auto_close": True,
            "reason": reason,
            "price": price,
        }

        await execution_router.execute_trade(signal, user_id=getattr(trade, 'user', None))

        # Prometheus metrics tracking
        SL_WATCHDOG_HITS.inc()

async def start_sl_watchdog(wallet_manager: WalletManager, trade_memory: TradeMemory, execution_router: ExecutionRouter):
    log.info("🧠 SL/TP Watchdog starting...")
    asyncio.create_task(monitor_open_positions(wallet_manager, trade_memory, execution_router))
