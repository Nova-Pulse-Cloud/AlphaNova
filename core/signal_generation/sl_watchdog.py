"""
Nova Pulse ALPHA – SL/TP Watchdog 2.0 (Nuclear Version)
────────────────────────────────────────
• Async, non-blocking sentinel for auto closing positions at TP/SL
• Uses PricingUtils for live price feeds and WalletManager + ExecutionRouter
• Pydantic validation of trade objects
• Prometheus metrics for errors, hits, and latency
• Configurable poll interval via environment
• Graceful shutdown and DLQ for failure replay
• Now: event firing, closed/pending memory marking, full audit trail logging
"""
import asyncio
import logging
from typing import Any, Dict

from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.utils.pricing_utils import PricingUtils
from core.wallet_management.wallet_manager import WalletManager
from core.execution.execution_router import ExecutionRouter
from core.utils.trade_memory import TradeMemory
from core.utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.sl_watchdog")
logger.setLevel(logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ───────────────────────────────────────────────────────────────────────────────
SL_WATCHDOG_ERRORS  = Counter("sl_watchdog_errors_total", "SL Watchdog errors")
SL_WATCHDOG_HITS    = Counter("sl_watchdog_hits_total", "TP/SL hit events")
SL_WATCHDOG_LATENCY = Histogram("sl_watchdog_latency_seconds", "SL/TP watch latency")

# ───────────────────────────────────────────────────────────────────────────────
# Data Models
# ───────────────────────────────────────────────────────────────────────────────
class OpenTrade(BaseModel):
    id: str
    symbol: str
    side: str
    entry: float = Field(..., ge=0)
    tp: float = Field(..., ge=0)
    sl: float = Field(..., ge=0)
    user: str
    size: float = Field(..., ge=0)
    status: str = "open"  # pending/closed tracking

# ───────────────────────────────────────────────────────────────────────────────
# Watchdog Implementation
# ───────────────────────────────────────────────────────────────────────────────
class SLWatchdog:
    def __init__(
        self,
        wallet_manager: WalletManager,
        trade_memory: TradeMemory,
        execution_router: ExecutionRouter,
        poll_interval: int = None
    ):
        self.wallet_manager    = wallet_manager
        self.trade_memory      = trade_memory
        self.execution_router  = execution_router
        self.pricing           = PricingUtils()
        self.poll_interval     = poll_interval or int(getattr(config, "SL_WATCHDOG_INTERVAL", 5))
        self._running          = False

    async def start(self) -> None:
        await self.pricing.connect()
        self._running = True
        logger.info("🛡️ SL/TP Watchdog started — polling every %s seconds", self.poll_interval)
        while self._running:
            await self._monitor_all()
            await asyncio.sleep(self.poll_interval)

    async def stop(self) -> None:
        self._running = False
        await self.pricing.disconnect()
        logger.info("🔕 SL/TP Watchdog stopped")

    async def _monitor_all(self) -> None:
        try:
            history = await asyncio.get_event_loop().run_in_executor(
                None, self.trade_memory.get_history
            )
            open_trades = [t for t in history if t.get("exit", 0) == 0]
            await asyncio.gather(*(self._monitor_trade(t) for t in open_trades), return_exceptions=True)
        except Exception as e:
            SL_WATCHDOG_ERRORS.inc()
            logger.error(f"[SLWatchdog] Monitoring loop error: {e}")

    async def _monitor_trade(self, raw: Dict[str, Any]) -> None:
        try:
            trade = OpenTrade(**raw)
        except ValidationError as ve:
            SL_WATCHDOG_ERRORS.inc()
            logger.error(f"[SLWatchdog] Invalid trade record: {ve}")
            return

        start_ts = asyncio.get_event_loop().time()
        try:
            price_data = await self.pricing.fetch_price(trade.symbol)
            current    = price_data.price
        except Exception as e:
            SL_WATCHDOG_ERRORS.inc()
            logger.error(f"[SLWatchdog] Price fetch error for {trade.symbol}: {e}")
            return

        logger.info(
            f"[SLWatchdog][AUDIT] {trade.symbol} side={trade.side} entry={trade.entry} "
            f"TP={trade.tp} SL={trade.sl} cur={current} user={trade.user} size={trade.size}"
        )

        hit_tp = (trade.side == "buy"  and current >= trade.tp) \
              or (trade.side == "sell" and current <= trade.tp)
        hit_sl = (trade.side == "buy"  and current <= trade.sl) \
              or (trade.side == "sell" and current >= trade.sl)

        # skip already-handled
        if trade.status in ("closed", "pending"):
            return

        if hit_tp or hit_sl:
            reason = "tp" if hit_tp else "sl"
            SL_WATCHDOG_HITS.inc()
            logger.info(f"[SLWatchdog] {reason.upper()} hit for {trade.symbol} at {current}")

            # mark pending
            await asyncio.get_event_loop().run_in_executor(
                None, self.trade_memory.mark_trade_pending, trade.id
            )

            # fire plugin event
            await fire_plugin_event("sl_tp_triggered", {
                "trade_id": trade.id,
                "symbol":   trade.symbol,
                "side":     trade.side,
                "trigger":  reason,
                "entry":    trade.entry,
                "tp":       trade.tp,
                "sl":       trade.sl,
                "current":  current,
                "user":     trade.user,
            })

            # execute auto-close
            close_sig = {
                "symbol":   trade.symbol,
                "side":     "sell" if trade.side == "buy" else "buy",
                "size":     trade.size,
                "auto_close": True,
                "reason":   reason,
                "price":    current,
                "entry":    trade.entry,
                "tp":       trade.tp,
                "sl":       trade.sl,
                "user":     trade.user,
                "trade_id": trade.id,
            }
            await self.execution_router.execute_trade(close_sig, user_id=trade.user)

            # finally mark closed
            await asyncio.get_event_loop().run_in_executor(
                None,
                self.trade_memory.mark_trade_closed,
                trade.id,
                current,
                reason,
                trade.tp,
                trade.sl,
                (current - trade.entry) * (trade.size if trade.side == "buy" else -trade.size)
            )

        SL_WATCHDOG_LATENCY.observe(asyncio.get_event_loop().time() - start_ts)

# ───────────────────────────────────────────────────────────────────────────────
# Entry point expected by main_service.py
# ───────────────────────────────────────────────────────────────────────────────
async def start_sl_watchdog(
    wallet_manager: WalletManager,
    trade_memory: TradeMemory,
    execution_router: ExecutionRouter,
    poll_interval: int = None
) -> None:
    """
    Launches the SL/TP watchdog loop.
    Used in main_service.py as:
        asyncio.create_task(start_sl_watchdog(wallet_manager, trade_memory, execution_router))
    """
    watchdog = SLWatchdog(wallet_manager, trade_memory, execution_router, poll_interval)
    await watchdog.start()
