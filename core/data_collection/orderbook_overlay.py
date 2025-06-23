# core/data_collection/orderbook_overlay.py

# Nova Pulse ALPHA – OrderBookOverlay 2.0
# -------------------------------------------------------------
# • Live L2 order-book depth for ANY SPL token via Jupiter API
# • Solana-native (no mock RPC) – works on Raydium/OpenBook pools
# • Async, cache-aware, Prometheus instrumented, SHA-256 provenance
# • Plug-ready for CombinedSignalEngine → slippage / TP-SL logic
# -------------------------------------------------------------

from __future__ import annotations
import asyncio
import hashlib
import logging
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx
from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.orderbook_overlay")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
OB_FETCH          = Counter("orderbook_fetch_total", "Total Jupiter order-book pulls")
OB_ERRORS         = Counter("orderbook_errors_total", "Order-book fetch/parse errors")
OB_LATENCY        = Histogram("orderbook_latency_seconds", "Latency of order-book pulls")
OB_CACHE_HITS     = Counter("orderbook_cache_hits_total", "Cache hits (≤ ttl)")
OB_CACHE_MISSES   = Counter("orderbook_cache_misses_total", "Cache misses (> ttl)")

# ────────────────────────────────────────────────────────────────────────────────
# In-mem cache (key → (timestamp, depth)) – ttl default 10 s
# ────────────────────────────────────────────────────────────────────────────────
_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_CACHE_TTL = 10.0  # seconds
_CACHE_LOCK = asyncio.Lock()

# Jupiter v6 order-book endpoint (pairAddress param)
JUP_OB_URL = "https://quote-api.jup.ag/v6/orderbook/{market}"

# depth_n controls how many levels we keep (Jupiter returns top 20 by default)
DEFAULT_DEPTH_N = 20

# -------------------------------------------------------------------
# helper: provenance hash (for audits)
# -------------------------------------------------------------------
def _hash(data: Dict[str, Any]) -> str:
    return hashlib.sha256(str(data).encode()).hexdigest()

# -------------------------------------------------------------------
# OrderBookOverlay class
# -------------------------------------------------------------------
class OrderBookOverlay:
    """Pulls live L2 depth from Jupiter for a Raydium/OpenBook market address.

    Args:
        market (str): On-chain market address.
    """
    def __init__(
        self,
        market: str,
        *,
        depth_threshold: float = 1000.0,
        depth_n: int = DEFAULT_DEPTH_N,
        ttl: float = _CACHE_TTL,
    ) -> None:
        self.market = market
        self.depth_threshold = depth_threshold
        self.depth_n = depth_n
        self.ttl = ttl

    async def get_orderbook_data(self) -> Dict[str, Any]:
        """Return cached (or freshly-fetched) order-book depth dict."""
        async with _CACHE_LOCK:
            ts, data = _CACHE.get(self.market, (0.0, {}))
            if time.time() - ts < self.ttl and data:
                OB_CACHE_HITS.inc()
                return data
            OB_CACHE_MISSES.inc()

        async with OB_LATENCY.time():
            try:
                logger.debug("[OrderBookOverlay] Fetching depth for %s", self.market)
                async with httpx.AsyncClient(timeout=8) as client:
                    url = JUP_OB_URL.format(market=self.market)
                    resp = await client.get(url)
                    resp.raise_for_status()
                OB_FETCH.inc()
                raw = resp.json()
            except Exception as e:
                OB_ERRORS.inc()
                logger.error("[OrderBookOverlay] fetch error %s: %s", self.market, e)
                return {}

        depth = self._parse_depth(raw)
        async with _CACHE_LOCK:
            _CACHE[self.market] = (time.time(), depth)
        return depth

    async def get_orderbook_summary(self) -> Dict[str, Any]:
        """Returns trend + imbalance for signal engines."""
        depth = await self.get_orderbook_data()
        return self._analyse(depth)

    def _parse_depth(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        book = {"bids": [], "asks": []}
        for side in ("bids", "asks"):
            for lvl in raw.get(side, [])[: self.depth_n]:
                book[side].append((float(lvl["price"]), float(lvl["size"])))
        return book

    def _analyse(self, depth: Dict[str, Any]) -> Dict[str, Any]:
        if not depth.get("bids") or not depth.get("asks"):
            return {"imbalance_score": 0.0, "trend": "neutral", "hash": _hash(depth)}

        bid_notional = sum(p * q for p, q in depth["bids"])
        ask_notional = sum(p * q for p, q in depth["asks"])
        imbalance = bid_notional - ask_notional
        trend = "neutral"
        if imbalance > self.depth_threshold:
            trend = "bullish"
        elif imbalance < -self.depth_threshold:
            trend = "bearish"

        return {
            "imbalance_score": imbalance,
            "trend": trend,
            "depth_n": self.depth_n,
            "hash": _hash(depth),
        }

# -------------------------------------------------------------------
# Convenience helper for signal engines
# -------------------------------------------------------------------
async def get_orderbook_imbalance(market: str) -> Tuple[float, Dict[str, Any]]:
    ob = OrderBookOverlay(market)
    summary = await ob.get_orderbook_summary()
    return summary.get("imbalance_score", 0.0), summary
