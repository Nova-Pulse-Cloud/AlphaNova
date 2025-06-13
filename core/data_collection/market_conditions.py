import os
import json
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
import hashlib

import aioredis
import aiohttp
import httpx
from pydantic import BaseModel
from prometheus_client import Counter, Histogram

from core.plugin_manager import fire_plugin_event  # For plugin integration
from utils.config import SESSION_HEADERS, DEXSCREENER_URL, VOLUME_THRESHOLD, PRICE_CHANGE_THRESHOLD, MC_ERRORS, MC_REQUESTS
from core.orderbook_overlay import get_orderbook_imbalance  # Import OrderBookOverlay

logger = logging.getLogger("nova.market_conditions")

# Prometheus metrics
MC_REQUESTS = Counter("market_conditions_requests_total", "Total market condition requests")
MC_ERRORS = Counter("market_conditions_errors_total", "Total market condition errors")
MC_LATENCY = Histogram("market_conditions_latency_seconds", "Latency of market condition fetches")
MC_CACHE_HITS = Counter("market_conditions_cache_hits_total", "Cache hits for market conditions")
MC_DLQ = Counter("market_conditions_dlq_total", "Failed market conditions in DLQ")

# === Market Conditions Data Models ===
class MacroEvent(BaseModel):
    name: str
    date: str
    description: str
    impact: str

class MarketConditions(BaseModel):
    symbol: str
    price: float
    volatility: float
    sentiment: float
    macro_events: Dict[str, MacroEvent]
    fractal_score: float
    composite_score: float
    timestamp: int
    health_score: Optional[float]
    orderbook_imbalance: Optional[float]  # New field for orderbook imbalance
    provenance_hash: Optional[str]

# === MarketConditionsService Class ===
class MarketConditionsService:
    """
    Nova ALPHA: Market conditions engine with async, plugin overlays, cache, DLQ, Prometheus, audit hash.
    """
    def __init__(self, redis_url: str, gecko_api_url: str, macro_api_url: str, sentiment_channel: str = "sentiment_channel", cache_ttl: int = 60):
        self.redis_url = redis_url
        self.gecko_api_url = gecko_api_url
        self.macro_api_url = macro_api_url
        self.sentiment_channel = sentiment_channel
        self.cache_ttl = cache_ttl
        self.redis = None
        self.dlq = []

    async def _get_redis(self) -> Optional[aioredis.Redis]:
        """Lazily connect to Redis and return the client."""
        try:
            if self.redis is None:
                self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            return self.redis
        except Exception as e:
            MC_ERRORS.inc()
            self.dlq.append({"stage": "redis_init", "error": str(e)})
            logger.critical(f"[Redis Init] Failed: {e}")
            return None

    async def get_market_health(self) -> Dict[str, Any]:
        """
        Returns a quick market "health" score (avg. strong pairs, volatility, etc) from DEXScreener API.
        """
        MC_REQUESTS.inc()
        with MC_LATENCY.time():
            try:
                async with aiohttp.ClientSession(headers=SESSION_HEADERS) as session:
                    async with session.get(DEXSCREENER_URL, timeout=10) as resp:
                        if resp.status != 200:
                            MC_ERRORS.inc()
                            return {"status": "error", "reason": f"DEX API {resp.status}"}
                        data = await resp.json()
                        pairs = data.get("pairs", [])
                        strong = [p for p in pairs if self.is_strong_market(p)]
                        score = len(strong) / max(len(pairs), 1)
                        vol = sum(float(p.get("volume", 0) or 0) for p in pairs) / max(len(pairs), 1)
                        change = sum(float(p.get("priceChange", 0) or 0) for p in pairs) / max(len(pairs), 1)

                        redis = await self._get_redis()
                        if redis:
                            await redis.setex("market_health_cache", self.cache_ttl, json.dumps({
                                "score": score,
                                "avg_vol": vol,
                                "avg_change": change,
                                "timestamp": int(asyncio.get_event_loop().time()),
                            }))
                            MC_CACHE_HITS.inc()

                        return {
                            "status": "ok",
                            "score": score,
                            "strong_pairs": len(strong),
                            "total_pairs": len(pairs),
                            "avg_24h_vol": vol,
                            "avg_24h_change": change,
                        }
            except Exception as e:
                MC_ERRORS.inc()
                self.dlq.append({"stage": "market_health", "error": str(e)})
                logger.exception(f"[MarketHealth] Exception: {e}")
                return {"status": "error", "reason": str(e)}

    def is_strong_market(self, pair: dict) -> bool:
        """
        Determines if a DEXScreener pair is "strong" for score aggregation.
        """
        try:
            vol = float(pair.get("volume", 0) or 0)
            change = float(pair.get("priceChange", 0) or 0)
            return vol >= VOLUME_THRESHOLD and abs(change) >= PRICE_CHANGE_THRESHOLD
        except Exception:
            return False

    async def get_macro_events(self) -> List[MacroEvent]:
        """
        Fetch macro events/news from macro API, parse into MacroEvent objects.
        """
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(self.macro_api_url, timeout=8)
                data = resp.json()
                return [MacroEvent(**event) for event in data.get("events", [])]
        except Exception as e:
            MC_ERRORS.inc()
            self.dlq.append({"stage": "macro_api", "error": str(e)})
            logger.warning(f"[Macro] API error: {e}")
            return []

    async def get_full_market_conditions(self, symbol: str) -> MarketConditions:
        """
        Get the full suite of market conditions for a given symbol (extend for plugin overlays).
        """
        health = await self.get_market_health()
        macros = await self.get_macro_events()
        now = int(asyncio.get_event_loop().time())

        # Get orderbook imbalance data
        orderbook_imbalance = await get_orderbook_imbalance(symbol)

        # PLUG-IN: Wire in real price, volatility, sentiment overlays for prod!
        mc = MarketConditions(
            symbol=symbol,
            price=0,  # <--- plug in live price source
            volatility=0,  # <--- plug in live realized vol
            sentiment=0,   # <--- plug in real sentiment
            macro_events={e.name: e for e in macros},
            fractal_score=0,  # <--- plug in AI/ML fractal overlay
            composite_score=health.get("score", 0) if health.get("status") == "ok" else 0,
            timestamp=now,
            health_score=health.get("score", 0) if health.get("status") == "ok" else None,
            orderbook_imbalance=orderbook_imbalance  # Use OrderBook data
        )

        mc.provenance_hash = hashlib.sha256(
            f"{symbol}{mc.composite_score}{mc.health_score}{orderbook_imbalance}{now}".encode()
        ).hexdigest()

        return mc
