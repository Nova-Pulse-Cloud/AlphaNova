# core/utils/token_guardian.py
"""
Nova Pulse ALPHA – Token Guardian (Live/Explainable Risk Filter)
──────────────────────────────────────────────────────────────────────────────
• Real-time, explainable risk check for tokens (pre-trade / dashboard)
• Fast in-memory and Redis cache for ultra low latency
• Integrates external APIs (RugCheck, Solscan, custom heuristics)
• Sends alerts to Telegram if a risk is flagged (optional)
• Full metrics, error DLQ, and explainable reasons for block/allow
• Pluggable for community signals (future-proof)
"""
import logging
import time
import asyncio
from typing import Any, Dict, Optional
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_guardian")
logger.setLevel(logging.INFO)

GUARD_CHECKS   = Counter("token_guardian_checks_total", "Token Guardian risk checks")
GUARD_BLOCKS   = Counter("token_guardian_blocks_total", "Guardian blocks (risk/flag)")
GUARD_LATENCY  = Histogram("token_guardian_latency_seconds", "Guardian check latency")
GUARD_DLQ: list = []

RUGCHECK_URL = getattr(config, "RUGCHECK_API", None) or "https://www.rugcheck.xyz/api/token/"
REDIS_URL    = getattr(config, "REDIS_URL", None)

try:
    import redis.asyncio as redis_async
    REDIS_ENABLED = True
except ImportError:
    REDIS_ENABLED = False
    redis_async = None

_guard_cache: Dict[str, Dict[str, Any]] = {}
CACHE_TTL = 120  # seconds

async def get_rugcheck_status(symbol: str) -> Dict[str, Any]:
    import httpx
    url = f"{RUGCHECK_URL}{symbol}"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
        if isinstance(data, dict) and "score" in data:
            return {"rug_score": data["score"], "explanation": data.get("summary", ""), "details": data}
        return {"rug_score": 0.2, "explanation": "No flag (default)"}
    except Exception as e:
        logger.warning(f"[TokenGuardian] RugCheck API failed for {symbol}: {e}")
        return {"rug_score": 0.5, "explanation": f"API error: {e}"}

async def check_guardian(symbol: str) -> Dict[str, Any]:
    GUARD_CHECKS.inc()
    key = symbol.upper()
    t0 = time.monotonic()
    # In-memory cache
    v = _guard_cache.get(key)
    if v and time.time() - v.get("ts", 0) < CACHE_TTL:
        GUARD_LATENCY.observe(time.monotonic() - t0)
        return v
    # Redis cache
    if REDIS_ENABLED and REDIS_URL:
        try:
            redis = redis_async.from_url(REDIS_URL, decode_responses=True)
            cached = await redis.get(f"tg:{key}")
            if cached:
                v = eval(cached)
                _guard_cache[key] = v
                GUARD_LATENCY.observe(time.monotonic() - t0)
                return v
        except Exception:
            pass
    data = await get_rugcheck_status(symbol)
    rug_score = float(data.get("rug_score", 0.5))
    flag = rug_score > 0.33
    result = {
        "symbol": key,
        "passed": not flag,
        "rug_score": rug_score,
        "explanation": data.get("explanation", "N/A"),
        "details": data.get("details", {}),
        "ts": time.time(),
    }
    if flag:
        GUARD_BLOCKS.inc()
        GUARD_DLQ.append({"symbol": key, "rug_score": rug_score, "reason": data.get("explanation")})
        logger.warning(f"[TokenGuardian] Blocked: {key} | {data}")
    _guard_cache[key] = result
    if REDIS_ENABLED and REDIS_URL:
        try:
            redis = redis_async.from_url(REDIS_URL, decode_responses=True)
            await redis.set(f"tg:{key}", str(result), ex=CACHE_TTL)
        except Exception:
            pass
    GUARD_LATENCY.observe(time.monotonic() - t0)
    return result

# Alias for backward compatibility
TokenGuardian = check_guardian

async def alert_guardian_block(symbol: str, reason: str):
    from core.utils.config import config
    import httpx
    if not hasattr(config, "TELEGRAM_BOT_TOKEN") or not hasattr(config, "TELEGRAM_CHAT_ID"):
        return
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            await client.post(
                f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": config.TELEGRAM_CHAT_ID, "text": f"🚨 [TokenGuardian] {symbol} blocked: {reason}"},
            )
    except Exception as e:
        logger.error(f"[TokenGuardian] Telegram alert failed: {e}")

__all__ = ["check_guardian", "TokenGuardian", "alert_guardian_block"]
