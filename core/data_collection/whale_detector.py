# core/data_collection/whale_detector.py

"""
Nova Pulse ALPHA – Whale-Detector 2.1
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
• Live Solana whale sweep via Helius v0 token-transfer endpoint
• Async TTL cache with Prometheus hit/miss telemetry
• Concurrency-safe and no placeholders
• Event-bus emit → whale_activity for plug-ins/Telegram feed
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.utils.plugin_manager import fire_plugin_event

# ────────────────────────────────────────────────────────────────────────────────
# Logging & Metrics
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("nova.whale_detector")
logger.setLevel(logging.INFO)

WHALE_API_CALLS = Counter("whale_api_calls_total", "Helius whale-API calls")
WHALE_API_ERRS  = Counter("whale_api_errors_total", "Errors from whale detector")
WHALE_API_LAT   = Histogram("whale_api_latency_seconds", "Latency of whale-API fetch")
WHALE_CACHE_HIT = Counter("whale_cache_hits_total", "Whale-detector cache hits")
WHALE_CACHE_MIS = Counter("whale_cache_misses_total", "Whale-detector cache misses")
WHALE_TX_TOTAL  = Counter("whale_tx_total", "Whale-sized transfers detected")

# ────────────────────────────────────────────────────────────────────────────────
# Config & Constants
# ────────────────────────────────────────────────────────────────────────────────
HELIUS_KEY     = config.HELIUS_API_KEY
HELIUS_ENDPOINT = "https://api.helius.xyz/v0/token-transfers"

_CACHE_TTL = 300  # seconds
_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_cache_lock = asyncio.Lock()

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────
def _cache_key(symbol: str, chain: str) -> str:
    return f"{chain}:{symbol.upper()}"

async def _read_cache(symbol: str, chain: str) -> Optional[Dict[str, Any]]:
    async with _cache_lock:
        ts, data = _cache.get(_cache_key(symbol, chain), (0.0, None))
        if data and (time.time() - ts) < _CACHE_TTL:
            WHALE_CACHE_HIT.inc()
            return data
        WHALE_CACHE_MIS.inc()
        return None

async def _write_cache(symbol: str, chain: str, data: Dict[str, Any]) -> None:
    async with _cache_lock:
        _cache[_cache_key(symbol, chain)] = (time.time(), data)

async def _fetch_solana_whales(symbol: str) -> List[Dict[str, Any]]:
    url = f"{HELIUS_ENDPOINT}?api-key={HELIUS_KEY}&symbol={symbol}&limit=20"
    async with aiohttp.ClientSession() as sess:
        async with sess.get(url, timeout=10) as resp:
            resp.raise_for_status()
            raw = await resp.json()
            return raw.get("transfers", [])

# ────────────────────────────────────────────────────────────────────────────────
# Public API
# ────────────────────────────────────────────────────────────────────────────────
async def get_recent_whale_activity(
    symbol: str,
    *,
    chain: str = "solana",
    min_amount: float = 10_000
) -> Dict[str, Any]:
    """Return recent whale transfers + score (0-1). Currently Solana only."""
    if chain != "solana":
        raise NotImplementedError("Only Solana chain implemented in v2.1")
    if not HELIUS_KEY:
        logger.warning("[WhaleDetector] Missing HELIUS_API_KEY – returning empty result")
        return {"whales": [], "score": 0.0, "error": "no_api_key"}

    if cached := await _read_cache(symbol, chain):
        return cached

    start = time.perf_counter()
    try:
        WHALE_API_CALLS.inc()
        transfers = await _fetch_solana_whales(symbol)
        whales = [
            {
                "from":   tx.get("from"),
                "to":     tx.get("to"),
                "amount": tx.get("amount"),
                "sig":    tx.get("signature"),
                "slot":   tx.get("slot"),
                "time":   tx.get("blockTime"),
            }
            for tx in transfers
            if tx.get("amount", 0) >= min_amount
        ]
        for _ in whales:
            WHALE_TX_TOTAL.inc()

        score = min(len(whales) / 10, 1.0)
        payload = {
            "symbol":     symbol,
            "whales":     whales,
            "score":      score,
            "checked_at": datetime.utcnow().isoformat(),
        }

        await _write_cache(symbol, chain, payload)
        await fire_plugin_event("whale_activity", payload)
        return payload

    except Exception as e:
        WHALE_API_ERRS.inc()
        logger.error("[WhaleDetector] %s | %s", symbol, e)
        return {"whales": [], "score": 0.0, "error": str(e)}

    finally:
        WHALE_API_LAT.observe(time.perf_counter() - start)


async def get_recent_whale_tokens(
    *,
    limit: int = 6,
    chain: str = "solana"
) -> str:
    """Return a human-readable list of tokens with notable whale activity."""
    top = ["SOL", "USDC", "RAY", "JUP", "BONK", "PYTH"][:limit]
    lines: List[str] = []
    for sym in top:
        meta = await get_recent_whale_activity(sym, chain=chain)
        if meta.get("score", 0) > 0:
            lines.append(f"🐋 {sym} – score {meta['score']:.2f} – {len(meta['whales'])} tx")
    return "\n".join(lines) or "No recent whale activity."
