# File: core/signal_generation/sl_watchdog.py
# 🚀 Nova Pulse ALPHA – SL/TP Watchdog: Ultra-Reliable Position Sentinel

import asyncio
import logging
import httpx
from typing import Any, List

from core.wallet_management.wallet_manager import WalletManager
from core.execution.execution_router import ExecutionRouter
from utils.trade_memory import TradeMemory
from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.sl_watchdog")
logger.setLevel(logging.INFO)

# === Prometheus Metrics ===
SL_WATCHDOG_ERRORS  = Counter("sl_watchdog_errors_total", "SL Watchdog errors")
SL_WATCHDOG_HITS    = Counter("sl_watchdog_hits_total", "SL/TP hit events")
SL_WATCHDOG_LATENCY = Histogram("sl_watchdog_latency_seconds", "SL/TP watch latency")

POLL_INTERVAL = 8  # Seconds between price checks

async def get_live_price(symbol: str) -> float:
    """Fetch live price from CoinGecko (use utils.pricing_utils if available)."""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={symbol.lower()}&vs_currencies=usd"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.json()[symbol.lower()]["usd"]
    except Exception as e:
        logger.warning(f"[SL Watchdog] Price fetch error for {symbol}: {e}")
        SL_WATCHDOG_ERRORS.inc()
        raise

async def monitor_open_positions(
    wallet_manager: WalletManager,
    trade_memory: TradeMemory,
    execution_router: ExecutionRouter
):
    """Continuously scan open positions and spawn per-trade watchers."""
    while True:
        try:
            open_trades: List[Any] = await trade_memory.get_open_trades()
            tasks = [
                monitor_trade(tr, wallet_manager, trade_memory, execution_router)
                for tr in open_trades
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.warning(f"[SL Watchdog] Monitoring loop error: {e}")
            SL_WATCHDOG_ERRORS.inc()
        await asyncio.sleep(POLL_INTERVAL)

async def monitor_trade(
    trade: Any,
    wallet_manager: WalletManager,
    trade_memory: TradeMemory,
    execution_router: ExecutionRouter
):
    """Watch a single trade for TP/SL hits and auto-close if triggered."""
    symbol = getattr(trade, "symbol", None)
    entry  = getattr(trade, "entry", None)
    sl     = getattr(trade, "sl", None)
    tp     = getattr(trade, "tp", None)
    side   = getattr(trade, "side", None)
    user   = getattr(trade, "user", None)

    start = asyncio.get_event_loop().time()
    try:
        price = await get_live_price(symbol)
    except Exception:
        return

    # Determine hit conditions
    hit_tp = tp is not None and ((price >= tp) if side == "buy" else (price <= tp))
    hit_sl = sl is not None and ((price <= sl) if side == "buy" else (price >= sl))

    if hit_tp or hit_sl:
        reason = "tp" if hit_tp else "sl"
        logger.info(f"[SL Watchdog] Hit {reason} on {symbol} | entry={entry} | price={price}")

        signal = {
            "symbol": symbol,
            "entry": entry,
            "side": "sell" if side == "buy" else "buy",
            "score": 1.0,
            "auto_close": True,
            "reason": reason,
            "price": price,
        }
        try:
            await execution_router.execute_trade(signal, user_id=user)
            SL_WATCHDOG_HITS.inc()
        except Exception as exec_err:
            logger.error(f"[SL Watchdog] Auto-close failed for {symbol}: {exec_err}")
            SL_WATCHDOG_ERRORS.inc()
    finally:
        SL_WATCHDOG_LATENCY.observe(asyncio.get_event_loop().time() - start)

async def start_sl_watchdog(
    wallet_manager: WalletManager,
    trade_memory: TradeMemory,
    execution_router: ExecutionRouter
):
    """Kick off the SL/TP watchdog in the background."""
    logger.info("🧠 Starting SL/TP Watchdog...")
    asyncio.create_task(
        monitor_open_positions(wallet_manager, trade_memory, execution_router)
    )
