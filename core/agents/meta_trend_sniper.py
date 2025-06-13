import aiohttp
import logging
import hashlib
from datetime import datetime
from typing import Optional, Dict, Any, Callable, Awaitable
from prometheus_client import Counter, Histogram
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
import asyncio

# === Prometheus metrics ===
META_TREND_CALLS = Counter("meta_trend_calls_total", "Meta trend fetches")
META_TREND_ERRORS = Counter("meta_trend_errors_total", "Meta trend errors")
META_TREND_LATENCY = Histogram("meta_trend_latency_seconds", "Meta trend latency")
META_TREND_BREAKOUT = Counter("meta_trend_breakout_total", "Detected meta breakouts")
META_TREND_DLQ = Counter("meta_trend_dlq_total", "Meta trend DLQ entries")

# === DLQ and event hooks ===
FAILED_MT_DLQ = []
meta_trend_event_hooks: list[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []
def register_meta_trend_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    """
    Register an event hook for meta trend events (for mesh, alert, ops).
    """
    meta_trend_event_hooks.append(hook_fn)
async def fire_meta_trend_event(event_name: str, data: dict):
    """
    Fire a meta trend async event.
    """
    for hook in meta_trend_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            log.error(f"[Nova][MetaTrendSniper] Event hook failed [{hook.__name__}]: {e}")

log = logging.getLogger("nova.meta_trend")
DEXSCREENER_API = "https://api.dexscreener.com/latest/dex/tokens/"

def adaptive_weights(symbol: str, user: Optional[str] = None) -> Dict[str, float]:
    """
    RL/meta: Learn best weighting per pair, per user, etc.
    """
    return {"h1": 0.4, "h6": 0.3, "h24": 0.3}

def trend_hash(symbol: str, result: dict) -> str:
    """
    Audit hash for any trend result.
    """
    m = hashlib.sha256()
    m.update(symbol.encode())
    m.update(str(result).encode())
    return m.hexdigest()

def is_anomalous_trend(trend: dict) -> bool:
    """
    Anomaly/spoof/breakout detection for trends.
    """
    if trend["strength"] > 25 or trend["volume"] > 1_000_000: return True
    if trend["strength"] < -15: return True
    return False

TREND_CACHE: Dict[str, dict] = {}

trend_plugins: Dict[str, Callable[[str, Dict[str, float]], Awaitable[dict]]] = {}
def register_trend_plugin(name: str, fn: Callable[[str, Dict[str, float]], Awaitable[dict]]):
    """
    Register a trend source/plugin for mesh/trend fusion.
    """
    trend_plugins[name] = fn

async def detect_meta_trends(
    mint: str,
    user: Optional[str] = None,
    force_refresh: bool = False
) -> dict:
    """
    Async, mesh/plugin-based trend signal detector for Solana tokens.
    """
    META_TREND_CALLS.inc()
    cache_key = f"trend:{user or 'global'}:{mint}"
    if cache_key in TREND_CACHE and not force_refresh:
        return TREND_CACHE[cache_key]
    weights = adaptive_weights(mint, user)
    params = {"mint": mint, "user": user}
    try:
        async with META_TREND_LATENCY.time():
            async for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 5)):
                with attempt:
                    trend_fn = trend_plugins.get("dexscreener", _dexscreener_trend)
                    trend = await trend_fn(mint, weights)
                    trend["audit_hash"] = trend_hash(mint, trend)
                    if is_anomalous_trend(trend):
                        META_TREND_BREAKOUT.inc()
                        META_TREND_DLQ.inc()
                        FAILED_MT_DLQ.append({"mint": mint, "trend": trend})
                        await fire_meta_trend_event("trend_anomaly", {**params, **trend})
                    else:
                        await fire_meta_trend_event("trend_detected", {**params, **trend})
                    TREND_CACHE[cache_key] = trend
                    return trend
    except Exception as e:
        META_TREND_ERRORS.inc()
        META_TREND_DLQ.inc()
        FAILED_MT_DLQ.append({"mint": mint, "error": str(e)})
        await fire_meta_trend_event("trend_failed", {**params, "error": str(e)})
        return {"error": str(e), **params}

async def _dexscreener_trend(mint: str, weights: Dict[str, float]) -> dict:
    """
    DexScreener trend plugin (default).
    """
    url = DEXSCREENER_API + mint
    async with aiohttp.ClientSession() as session:
        async with session.get(url, timeout=8) as r:
            if r.status != 200:
                raise ValueError("Invalid DEXScreener response")
            data = await r.json()
            if not data.get("pairs"):
                raise ValueError("No pairs returned")
            pair = data["pairs"][0]
            price = float(pair.get("priceUsd", 0.0))
            volume = float(pair.get("volume", 0.0))
            price_change = pair.get("priceChange", {})
            h1 = float(price_change.get("h1", 0.0))
            h6 = float(price_change.get("h6", 0.0))
            h24 = float(price_change.get("h24", 0.0))
            strength = h1 * weights["h1"] + h6 * weights["h6"] + h24 * weights["h24"]
            whale = "Accumulating" if h6 > 5 and volume > 10_000 else "Flat"
            note = "🔥 Momentum breakout" if strength > 10 else "Neutral"
            cycle = "Bull Phase" if strength > 5 else "Neutral"
            return {
                "price": price,
                "volume": volume,
                "strength": round(strength, 2),
                "whale": whale,
                "cycle": cycle,
                "confidence": min(100.0, max(0.0, strength * 2)),
                "note": note,
                "timestamp": datetime.utcnow().isoformat()
            }

async def replay_dlq() -> None:
    """
    Retry all failed meta trend signals in DLQ.
    """
    for failed in FAILED_MT_DLQ[:]:
        res = await detect_meta_trends(failed["mint"], force_refresh=True)
        if res and not res.get("error"):
            FAILED_MT_DLQ.remove(failed)
            log.info(f"[Nova][MetaTrendSniper] DLQ meta trend succeeded and removed: {failed}")

def get_dlq() -> list:
    """
    Get a copy of the current DLQ.
    """
    return FAILED_MT_DLQ.copy()
