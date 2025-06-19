# File: core/agents/adaptive_tp_sl.py

import logging
import asyncio
from time import monotonic
from typing import Dict, Any, Optional

from prometheus_client import Gauge, Counter

from core.utils.config import config  # FIXED: import singleton, not class!
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_learning import RLTrader

logger = logging.getLogger("nova.adaptive_tp_sl")
logger.setLevel(logging.INFO)

# Prometheus metrics
TP_SL_LATENCY = Gauge("tp_sl_latency_seconds", "Time taken to compute adaptive TP/SL", ["symbol"])
TP_SL_UPDATED = Gauge("tp_sl_updated_total", "Total number of times TP/SL were updated", ["symbol"])
TP_SL_ERRORS  = Counter("tp_sl_errors_total",  "Total number of TP/SL compute errors", ["symbol"])

class AdaptiveTPSL:
    """
    ALPHA AdaptiveTPSL – Nova Pulse Elite Engine

    Computes unique, context-driven TP, SL, RR, and position size for each trade.
    Pulls live context, memory, and RL adjustments for maximal edge.
    """

    # Class-level constants (configurable via code changes)
    BASE_TP_ADJ       = 1.8
    BASE_SL_ADJ       = 0.9
    TP_LOWER_MULT     = 1.01
    TP_UPPER_MULT     = 2.0
    SL_LOWER_MULT     = 0.01
    SL_UPPER_MULT     = 0.99
    RR_MIN            = 0.5
    RR_MAX            = 2.6
    GAS_COST_MULT     = 2.4
    RUG_SCORE_MULT    = 0.45
    RUG_SCORE_CAP     = 0.33
    REGRET_CAP        = 0.12
    MIN_SIZE          = 0.001
    TP_SL_PRECISION   = 8
    SIZE_PRECISION    = 6

    def __init__(
        self,
        memory: Optional[TradeMemory] = None,
        rl_agent: Optional[RLTrader] = None,
        overlays: Optional[Dict[str, Any]] = None
    ):
        self.memory    = memory
        self.rl_agent  = rl_agent
        self.overlays  = overlays or {}

    async def compute(
        self,
        symbol: str,
        price: float,
        signal_context: Dict[str, Any],
        user: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Compute optimal TP, SL, RR, and position size for a live trade.
        """
        start_time = monotonic()
        try:
            # --- Live context extraction ---
            vol    = float(signal_context.get("volatility", 0.02))
            trend  = float(signal_context.get("trend_strength", 0.5))
            whale  = float(signal_context.get("whale_pressure", 0.0))
            spread = float(signal_context.get("spread", 0.001))
            gas    = float(signal_context.get("gas_cost", 0.0))
            bias   = float(signal_context.get("orderbook_bias", 0.0))
            rug    = float(signal_context.get("rug_score", 0.0))
            phase  = float(signal_context.get("cycle_phase", 0.5))

            # --- Memory & RL adjustments ---
            last_outcome = self.memory.get_latest_trade(symbol) if self.memory else {}
            last_win     = bool(last_outcome.get("pnl", 0) > 0)
            regret       = float(last_outcome.get("regret", 0.0))
            user_risk    = float(signal_context.get("user_risk", 0.0))
            rl_mod       = float(self.rl_agent.get_signal_mod(symbol)) if self.rl_agent else 0.0

            # --- Dynamic risk biases ---
            risk_bias = -0.25 if not last_win else 0.15
            regret_adj = max(-self.REGRET_CAP, min(regret, self.REGRET_CAP))
            rug_penalty_adj = -0.15 if rug > 0.7 else 0.0

            adj_vol = vol * (1.0 + phase + abs(bias))

            # --- Compute base TP/SL ---
            base_tp = price * (
                1
                + adj_vol
                  * (
                      self.BASE_TP_ADJ
                      + trend
                      + risk_bias
                      + regret_adj
                      + user_risk
                      + rl_mod
                      - whale
                      + bias
                      - rug
                    )
            )
            base_sl = price * (
                1
                - adj_vol
                  * (
                      self.BASE_SL_ADJ
                      - risk_bias
                      + whale
                      + spread
                      + rug
                      - regret_adj
                      - bias
                    )
            )

            # --- Enforce TP/SL bounds and precision ---
            tp = round(
                max(price * self.TP_LOWER_MULT,
                    min(base_tp, price * self.TP_UPPER_MULT)
                ),
                self.TP_SL_PRECISION
            )
            sl = round(
                min(price * self.SL_UPPER_MULT,
                    max(base_sl, price * self.SL_LOWER_MULT)
                ),
                self.TP_SL_PRECISION
            )

            # --- Risk/Reward & size calculation ---
            risk   = price - sl
            reward = tp - price
            rr     = reward / risk if risk > 0 else float("inf")

            size = self._adjust_size(price, vol, rr, gas, rug, regret)

            logger.info(
                f"[AdaptiveTPSL] {symbol} | TP={tp} | SL={sl} | RR={rr:.3f} | Size={size}"
            )

            # --- Metrics reporting ---
            TP_SL_LATENCY.labels(symbol=symbol).set(monotonic() - start_time)
            TP_SL_UPDATED.labels(symbol=symbol).inc()

            return {
                "tp": tp,
                "sl": sl,
                "rr_ratio": rr,
                "size": size,
                "meta": {
                    "volatility": vol,
                    "trend_strength": trend,
                    "whale_pressure": whale,
                    "orderbook_bias": bias,
                    "rug_score": rug,
                    "cycle_phase": phase,
                    "last_win": last_win,
                    "regret": regret,
                    "rl_mod": rl_mod,
                    "user_risk": user_risk,
                },
            }

        except Exception as e:
            TP_SL_ERRORS.labels(symbol=symbol).inc()
            logger.error(f"[AdaptiveTPSL][ERROR] {symbol} | {e} | Context={signal_context}")
            raise

    def _adjust_size(
        self,
        price: float,
        vol: float,
        rr: float,
        gas: float,
        rug: float,
        regret: float
    ) -> float:
        """
        Compute dynamic position size based on volatility, RR, gas, rug score, and regret.
        """
        base_size    = 1.0 / (vol + 1e-6)
        rr_mod       = max(self.RR_MIN, min(rr, self.RR_MAX))
        rug_penalty  = 1.0 - min(self.RUG_SCORE_CAP, rug * self.RUG_SCORE_MULT)
        regret_pen   = 1.0 - min(self.REGRET_CAP, abs(regret))

        size = base_size * rr_mod * rug_penalty * regret_pen
        size = size * (1.0 - gas * self.GAS_COST_MULT)

        return max(self.MIN_SIZE, round(size, self.SIZE_PRECISION))

    async def benchmark_models(
        self,
        symbol: str,
        price: float,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Async benchmarking entry point for multiple TP/SL strategies.
        """
        result = await self.compute(symbol, price, context)
        return {"nova_adaptive": result}
