import math
import asyncio
from typing import Dict, Tuple, Optional

__version__ = "1.0.3"
__type__ = "tpsl_plugin"
__capabilities__ = ["tp_sl"]
__modes__ = ["live", "sim"]

# Define constants for better configurability
DEFAULT_VOLATILITY = 0.02
DEFAULT_TREND_STRENGTH = 0.5
DEFAULT_WHALE_PRESSURE = 0.0
DEFAULT_SPREAD = 0.001
DEFAULT_TP_BASE = 0.04
DEFAULT_SL_BASE = 0.03
MIN_TP = 0.015
MAX_TP = 0.25
MIN_SL = 0.01
MAX_SL = 0.2
AGGRESSIVE_SL_THRESHOLD = 0.2
AGGRESSIVE_SL_MULTIPLIER = 1.5

async def jupiter_tp_sl_plugin(features: Dict[str, any], alpha_signal: Dict[str, any], user: Optional[str] = None) -> Tuple[float, float, Dict]:
    """
    Signal-aware dynamic Take Profit (TP) and Stop Loss (SL) engine using market depth,
    volatility, trend strength, whale pressure, and signal features like score and risk.

    This function calculates dynamic TP and SL percentages based on these market features,
    integrating the alpha signal’s score and risk.
    """
    # Get features from the input or use default values
    entry_price = features.get("entry_price", 0)
    volatility = features.get("volatility", DEFAULT_VOLATILITY)
    trend_strength = features.get("trend_strength", DEFAULT_TREND_STRENGTH)
    whale_pressure = features.get("whale_pressure", DEFAULT_WHALE_PRESSURE)
    spread = features.get("spread", DEFAULT_SPREAD)

    # Ensure entry price is valid
    if entry_price <= 0:
        raise ValueError("Invalid entry price. Must be greater than zero.")

    # Extract alpha signal data
    score = alpha_signal.get("score", 0.5)  # Default to 0.5 if not present
    risk = alpha_signal.get("risk", 0.5)    # Default to 0.5 if not present
    flavor = alpha_signal.get("flavor", "Moderate")  # Default to 'Moderate'

    # Adjust volatility factor dynamically
    volatility_factor = max(0.02, min(0.1, volatility))  # Dynamically adjust based on actual volatility

    # Calculate dynamic TP based on score, risk, and flavor
    tp_pct = max(MIN_TP, min(MAX_TP, (DEFAULT_TP_BASE + score * 0.12 + whale_pressure * 0.07) * volatility_factor))

    # Calculate dynamic SL based on risk and volatility
    sl_pct = max(MIN_SL, min(MAX_SL, (DEFAULT_SL_BASE + (volatility * 1.3) + spread) * volatility_factor))

    # Flavor-based adjustments: "Safe" signals get wider TP, tighter SL; "Degen" signals get tighter TP, wider SL
    if flavor == "Safe":
        tp_pct *= 1.5  # Wider TP for safe signals
        sl_pct *= 0.8  # Tighter SL for safe signals
    elif flavor == "Degen":
        tp_pct *= 0.8  # Smaller TP for degen signals
        sl_pct *= 1.2  # Wider SL for degen signals

    # Aggressive SL logic if the trend is weak (i.e., trend strength is low)
    if trend_strength < AGGRESSIVE_SL_THRESHOLD:
        sl_pct *= AGGRESSIVE_SL_MULTIPLIER  # Make SL more aggressive

    # Adjust SL for low liquidity (high spread) and volume (market depth)
    volume = features.get("volume", 1000000)  # Placeholder for volume-based adjustments
    if volume < 1000000:  # Placeholder for low liquidity
        sl_pct *= 1.2  # Wider SL for low liquidity tokens

    # Metadata for logging or further analysis
    metadata = {
        "plugin": "jupiter",
        "trend_strength": trend_strength,
        "whale_pressure": whale_pressure,
        "volatility": volatility,
        "spread": spread,
        "entry_price": entry_price,
        "score": score,
        "risk": risk,
        "flavor": flavor,
        "tp_pct": tp_pct,
        "sl_pct": sl_pct,
        "volume": volume,
    }

    # Context switch (lightweight async pause for event loop)
    await asyncio.sleep(0)

    return tp_pct, sl_pct, metadata
