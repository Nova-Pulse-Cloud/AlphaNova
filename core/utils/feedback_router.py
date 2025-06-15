# core/utils/feedback_router.py

import os
import json
import asyncio
import logging
import time
import uuid
from typing import Any, Dict, List

from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.data_collection.market_conditions import MarketConditionsService
from core.wallet_management.wallet_manager import WalletManager

logger = logging.getLogger("nova.feedback_router")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ────────────────────────────────────────────────────────────────────────────────
RL_CYCLES = Counter("rl_trainer_cycles_total", "Total RL training cycles executed")
RL_ERRORS = Counter("rl_trainer_errors_total", "Errors encountered during RL training cycles")
RL_LATENCY = Histogram("rl_trainer_latency_seconds", "Latency of each RL training cycle")

class RLTrainerLoop:
    """
    Schedules and executes RL tuning cycles using live metrics
    (PnL, volatility, trend strength) with no placeholders.
    """
    def __init__(
        self,
        rl_tuner: Any,
        symbols: List[str] = None,
    ) -> None:
        # Core services
        self.rl_tuner = rl_tuner
        self.trade_memory = TradeMemory(config)
        self.market_svc = MarketConditionsService(config)
        self.wallet_mgr = WalletManager()

        # Schedule parameters
        self.interval = int(os.getenv("RL_TRAIN_INTERVAL", "900"))  # seconds
        self.symbols = symbols or os.getenv("RL_TRAIN_SYMBOLS", "SOL").split(",")

        self._running = False

    async def start(self) -> None:
        """Begin RL tuning loop until stopped."""
        self._running = True
        logger.info("[FeedbackRouter] RL Trainer Loop started, interval=%s sec", self.interval)
        while self._running:
            await self._run_cycle()
            await asyncio.sleep(self.interval)

    def stop(self) -> None:
        """Stop the RL training loop."""
        self._running = False
        logger.info("[FeedbackRouter] RL Trainer Loop stopped")

    async def _run_cycle(self) -> None:
        """Execute a single RL tuning cycle with live system data."""
        RL_CYCLES.inc()
        trace_id = f"rl-train-{uuid.uuid4().hex[:8]}"
        start_ts = time.time()

        try:
            # 1. Compute average PnL from trade history
            history = await asyncio.get_running_loop().run_in_executor(
                None, self.trade_memory.get_history
            )
            pnl_vals = [t.get("pnl", 0.0) for t in history]
            avg_pnl = sum(pnl_vals) / len(pnl_vals) if pnl_vals else 0.0

            # 2. Aggregate market metrics across symbols
            vols = []
            trends = []
            for sym in self.symbols:
                vols.append(await self.market_svc.get_volatility(sym))
                trends.append(await self.market_svc.get_trend_strength(sym))
            avg_volatility = sum(vols) / len(vols) if vols else 0.0
            avg_trend = sum(trends) / len(trends) if trends else 0.0

            # 3. Execute tuner
            param_grid_raw = os.getenv("RL_PARAM_GRID")
            if param_grid_raw:
                param_grid = json.loads(param_grid_raw)
            else:
                param_grid = {"alpha": [0.1, 0.3, 0.5], "beta": [0.2, 0.4, 0.6], "gamma": [0.95, 0.99]}

            ctx = {
                "avg_pnl": avg_pnl,
                "avg_volatility": avg_volatility,
                "avg_trend": avg_trend,
                "trace_id": trace_id,
                "timestamp": int(time.time()),
            }

            logger.info("[FeedbackRouter] Starting RL tuning cycle %s with ctx=%s", trace_id, ctx)
            result = await self.rl_tuner.tune(
                ctx=ctx,
                param_grid=param_grid,
                user="feedback_loop",
                simulation=os.getenv("RL_SIMULATION", "true").lower() == "true"
            )

            logger.info(
                "[FeedbackRouter] RL tuning complete [%s]: best_score=%s audit_hash=%s", 
                trace_id,
                result.get("best_score"),
                result.get("audit_hash")
            )

        except Exception as e:
            RL_ERRORS.inc()
            logger.error(f"[FeedbackRouter] Error in RL cycle {trace_id}: {e}")
        finally:
            RL_LATENCY.observe(time.time() - start_ts)

# Usage example:
# loop = RLTrainerLoop(your_rl_tuner)\# asyncio.create_task(loop.start())
