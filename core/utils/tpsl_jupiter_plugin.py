# core/utils/tpsl_jupiter_plugin.py

import logging
from time import monotonic
from typing import Any, Dict, Optional, Tuple

from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.orderbook_overlay import get_orderbook_imbalance

logger = logging.getLogger("nova.tpsl_jupiter_plugin")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
PLUGIN_INVOCATIONS = Counter(
    "tpsl_plugin_invocations_total", "Total TPSL plugin invocations"
)
PLUGIN_ERRORS = Counter(
    "tpsl_plugin_errors_total", "TPSL plugin errors"
)
PLUGIN_LATENCY = Histogram(
    "tpsl_plugin_latency_seconds", "Latency of TPSL plugin execution", ["mode"]
)

class JupiterTPSLPlugin:
    """
    Jupiter TP/SL Plugin—fully dynamic.

    Computes take-profit and stop-loss percentages based on live market data,
    including volatility, trend strength, whale behavior, and orderbook imbalance.
    """
    def __init__(self) -> None:
        self.market_svc = MarketConditionsService(config)

    async def compute(
        self,
        symbol: str,
        entry_price: float,
        alpha_signal: Dict[str, Any],
        mode: str = "live",
        user: Optional[str] = None
    ) -> Tuple[float, float, Dict[str, Any]]:
        """
        :returns: (tp_pct, sl_pct, metadata)
        """
        PLUGIN_INVOCATIONS.inc()
        start = monotonic()
        try:
            # Fetch live metrics
            volatility     = await self.market_svc.get_volatility(symbol)
            trend_strength = await self.market_svc.get_trend_strength(symbol)
            whale_pressure = await get_recent_whale_activity(symbol)
            spread, _      = await get_orderbook_imbalance(symbol)

            # Alpha inputs
            score  = float(alpha_signal.get("score", 0.5))
            risk   = float(alpha_signal.get("risk", 0.5))
            flavor = alpha_signal.get("flavor", "Moderate")

            # Dynamic calculations
            tp_factor = 1 + volatility * (1 + score + trend_strength - whale_pressure)
            sl_factor = 1 - volatility * (1 + risk + spread + whale_pressure)

            # Flavor adjustments
            if flavor.lower() == "safe":
                tp_factor *= 1.2
                sl_factor *= 0.8
            elif flavor.lower() == "degen":
                tp_factor *= 0.8
                sl_factor *= 1.2

            # Final percentages
            tp_pct = max(0.0, tp_factor - 1.0)
            sl_pct = max(0.0, 1.0 - sl_factor)

            metadata: Dict[str, Any] = {
                "symbol": symbol,
                "entry_price": entry_price,
                "volatility": volatility,
                "trend_strength": trend_strength,
                "whale_pressure": whale_pressure,
                "spread": spread,
                "score": score,
                "risk": risk,
                "flavor": flavor,
                "mode": mode,
                "user": user,
            }
            PLUGIN_LATENCY.labels(mode=mode).observe(monotonic() - start)
            return tp_pct, sl_pct, metadata
        except Exception as e:
            PLUGIN_ERRORS.inc()
            logger.error(f"[TPSL Plugin] Error for {symbol}: {e}")
            PLUGIN_LATENCY.labels(mode=mode).observe(monotonic() - start)
            raise

# Alias for functional import
async def jupiter_tp_sl_plugin(
    features: Dict[str, Any],
    alpha_signal: Dict[str, Any],
    mode: str = "live",
    user: Optional[str] = None
) -> Tuple[float, float, Dict[str, Any]]:
    plugin = JupiterTPSLPlugin()
    return await plugin.compute(
        symbol=features.get("symbol"),
        entry_price=features.get("entry_price"),
        alpha_signal=alpha_signal,
        mode=mode,
        user=user
    )
