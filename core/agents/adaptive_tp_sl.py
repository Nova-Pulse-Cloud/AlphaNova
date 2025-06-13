import math
import logging
from typing import Dict, Any, Optional
import asyncio
from prometheus_client import Gauge

logger = logging.getLogger("nova.adaptive_tp_sl")
logger.setLevel(logging.INFO)

# Prometheus metrics for real-time tracking
TP_SL_LATENCY = Gauge("tp_sl_latency_seconds", "Time taken to compute adaptive TP/SL", ["symbol"])
TP_SL_UPDATED = Gauge("tp_sl_updated_total", "Total number of times TP/SL were updated", ["symbol"])

class AdaptiveTPSL:
    """
    ALPHA AdaptiveTPSL – Nova Pulse Elite Engine

    Computes unique, context-driven take-profit (TP), stop-loss (SL), and ultra-optimized
    position size for each trade. Leverages live context, memory, and RL agent for maximal edge.
    
    Args:
        memory: State memory for trade outcomes (must be live, no mock).
        rl_agent: RL agent for dynamic risk/reward modulation (must be live, no mock).
        overlays: Dict of additional, live signal overlays (optional).
    """

    def __init__(self, memory: Optional[Any] = None, rl_agent: Optional[Any] = None, overlays: Optional[Dict[str, Any]] = None):
        self.memory = memory
        self.rl_agent = rl_agent
        self.overlays = overlays or {}

    async def compute(self, symbol: str, price: float, signal_context: Dict[str, Any], user: Optional[str] = None) -> Dict[str, Any]:
        """
        Compute optimal TP, SL, RR, and size for a single live trade.
        Must only be called with live, real signal_context.
        """
        start_time = asyncio.get_event_loop().time()
        
        try:
            # --- Extract live metrics
            volatility = float(signal_context.get("volatility", 0.02))
            trend_strength = float(signal_context.get("trend_strength", 0.5))
            whale_meta = float(signal_context.get("whale_pressure", 0.0))
            spread = float(signal_context.get("spread", 0.001))
            gas_cost = float(signal_context.get("gas_cost", 0.0))
            orderbook_bias = float(signal_context.get("orderbook_bias", 0.0))
            rug_score = float(signal_context.get("rug_score", 0.0))
            cycle_phase = float(signal_context.get("cycle_phase", 0.5))

            # --- State/memory impact
            memory_mod = self.memory.get_last_outcome(symbol) if self.memory else {}
            last_win = bool(memory_mod.get("last_win", True))
            regret_score = float(memory_mod.get("regret", 0.0))
            user_risk_bias = float(signal_context.get("user_risk", 0.0))
            rl_mod = float(self.rl_agent.get_signal_mod(symbol)) if self.rl_agent else 0.0

            # --- Risk/Reward dynamic adjustment (Live-Alpha)
            risk_bias = -0.25 if not last_win else 0.15  # Slightly more aggressive after wins
            regret_adj = max(-0.12, min(regret_score, 0.12))  # Alpha-tuned regret bounds
            rug_adj = -0.15 if rug_score > 0.7 else 0.0

            adj_vol = volatility * (1.0 + cycle_phase + abs(orderbook_bias))

            # --- Calculate adaptive TP/SL (Alpha dynamic formulas)
            base_tp = price * (1 + adj_vol * (1.8 + trend_strength + risk_bias + regret_adj + user_risk_bias + rl_mod - whale_meta + orderbook_bias - rug_score))
            base_sl = price * (1 - adj_vol * (0.9 - risk_bias + whale_meta + spread + rug_score - regret_adj - orderbook_bias))

            # --- Always guarantee SL/TP min/max bounds
            tp = round(max(price * 1.01, min(base_tp, price * 2.0)), 8)
            sl = round(min(price * 0.99, max(base_sl, price * 0.01)), 8)

            risk = price - sl
            reward = tp - price
            rr_ratio = reward / risk if risk > 0 else float('inf')

            optimal_size = self._adjust_size(price, volatility, rr_ratio, gas_cost, rug_score, regret_score)

            logger.info(f"[Nova-ALPHA][AdaptiveTPSL] {symbol} | TP={tp:.8f} | SL={sl:.8f} | RR={rr_ratio:.3f} | Size={optimal_size:.6f} | Phase={cycle_phase:.2f}")

            # Record performance metrics for monitoring
            TP_SL_LATENCY.labels(symbol=symbol).set(asyncio.get_event_loop().time() - start_time)
            TP_SL_UPDATED.labels(symbol=symbol).inc()

            return {
                "tp": tp,
                "sl": sl,
                "rr_ratio": rr_ratio,
                "size": optimal_size,
                "volatility": volatility,
                "whale_meta": whale_meta,
                "trend_strength": trend_strength,
                "spread": spread,
                "gas_cost": gas_cost,
                "orderbook_bias": orderbook_bias,
                "rug_score": rug_score,
                "cycle_phase": cycle_phase,
                "meta": {
                    "source": "nova.adaptive_tp_sl",
                    "used_memory": bool(self.memory),
                    "used_rl": bool(self.rl_agent),
                    "regret": regret_score,
                    "user_risk": user_risk_bias,
                    "rl_mod": rl_mod,
                    "live": True
                }
            }
        except Exception as e:
            logger.error(f"[Nova-ALPHA][AdaptiveTPSL][ERROR] {symbol} | {str(e)} | Context: {signal_context}")
            raise

    def _adjust_size(self, price: float, volatility: float, rr: float, gas_cost: float, rug_score: float, regret_score: float) -> float:
        """
        Alpha-tuned dynamic position size optimization for live trading only.
        """
        base_size = 1.0 / (volatility + 1e-6)
        rr_modifier = max(0.5, min(rr, 2.6))  # Alpha RR cap
        rug_penalty = 1.0 - min(0.33, rug_score * 0.45)
        regret_penalty = 1.0 - min(0.12, abs(regret_score))
        size = base_size * rr_modifier * rug_penalty * regret_penalty
        size = size * (1.0 - gas_cost * 2.4)
        return max(0.001, round(size, 6))

    def benchmark_models(self, symbol: str, price: float, context: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """
        For internal alpha benchmarking; always uses live context.
        """
        return {
            "nova_adaptive": self.compute(symbol, price, context)
        }
