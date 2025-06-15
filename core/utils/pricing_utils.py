# core/utils/pricing_utils.py

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import redis.asyncio as redis_client
import httpx
from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from prometheus_client import Counter, Histogram

from core.utils.config import config  # <-- Corrected import

logger = logging.getLogger("nova.pricing_utils")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
PRICE_FETCHES    = Counter("price_fetch_requests_total", "Total price fetch requests")
PRICE_ERRORS     = Counter("price_fetch_errors_total",   "Errors fetching prices")
CACHE_HITS       = Counter("price_cache_hits_total",     "Cache hits for price fetch")
CACHE_MISSES     = Counter("price_cache_misses_total",   "Cache misses for price fetch")
CACHE_LATENCY    = Histogram("price_cache_latency_seconds","Latency of cache operations")
HTTP_LATENCY     = Histogram("price_http_latency_seconds", "Latency of HTTP price requests")

# ────────────────────────────────────────────────────────────────────────────────
# Data Model
# ────────────────────────────────────────────────────────────────────────────────
class PriceQuote(BaseModel):
    symbol: str
    price: float = Field(..., gt=0)
    timestamp: int

# ────────────────────────────────────────────────────────────────────────────────
# Pricing Utility
# ────────────────────────────────────────────────────────────────────────────────
class PricingUtils:
    """
    Production-grade pricing utility:
    - Redis caching with TTL
    - Primary fetch from CoinMarketCap API
    - Fallback fetch from CoinGecko API
    - Async retry logic and Prometheus metrics
    """
    def __init__(self, cache_ttl: int = 5) -> None:
        self.redis_url    = config.REDIS_URL
        self.cmc_api_key  = config.CMC_API_KEY
        self.gecko_api    = config.GECKO_API_URL.rstrip('/')
        self.cache_ttl    = cache_ttl
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=10)

    async def connect(self) -> None:
        """Connect to Redis cache."""
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[PricingUtils] Connected to Redis")
        except Exception as e:
            logger.error(f"[PricingUtils] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        """Close HTTP and Redis connections."""
        await self.http.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[PricingUtils] Shutdown complete")

    async def fetch_price(self, symbol: str) -> PriceQuote:
        """Fetch current price for a symbol, with cache and fallback logic."""
        PRICE_FETCHES.inc()
        key = f"price:{symbol.upper()}"
        ts = int(datetime.utcnow().timestamp())
        # Check cache
        if self.redis:
            with CACHE_LATENCY.time():
                try:
                    raw = await self.redis.get(key)
                    if raw:
                        CACHE_HITS.inc()
                        return PriceQuote.parse_raw(raw)
                except Exception as e:
                    logger.warning(f"[PricingUtils] Redis get failed: {e}")
        CACHE_MISSES.inc()

        # Primary: CoinMarketCap
        cmc_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key}
        params = {"symbol": symbol.upper()}
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(3), wait=wait_exponential_jitter(1,5), reraise=True
            ):
                with HTTP_LATENCY.time():
                    resp = await self.http.get(cmc_url, headers=headers, params=params)
                    resp.raise_for_status()
            data = resp.json()
            price = data["data"][symbol.upper()]["quote"]["USD"]["price"]
            quote = PriceQuote(symbol=symbol.upper(), price=float(price), timestamp=ts)
        except Exception as e:
            PRICE_ERRORS.inc()
            logger.warning(f"[PricingUtils] CMC fetch failed for {symbol}: {e}")
            # Fallback: CoinGecko
            try:
                gecko_url = f"{self.gecko_api}/simple/price"
                async for attempt in AsyncRetrying(
                    stop=stop_after_attempt(3), wait=wait_exponential_jitter(1,5), reraise=True
                ):
                    with HTTP_LATENCY.time():
                        resp = await self.http.get(gecko_url, params={"ids": symbol.lower(), "vs_currencies":"usd"})
                        resp.raise_for_status()
                data = resp.json()
                price = data.get(symbol.lower(), {}).get("usd")
                if not price or price <= 0:
                    raise RuntimeError("Invalid price from Gecko")
                quote = PriceQuote(symbol=symbol.upper(), price=float(price), timestamp=ts)
            except Exception as e2:
                PRICE_ERRORS.inc()
                logger.error(f"[PricingUtils] Gecko fetch failed for {symbol}: {e2}")
                # Final fallback
                quote = PriceQuote(symbol=symbol.upper(), price=0.0, timestamp=ts)

        # Cache result
        if self.redis:
            try:
                await self.redis.set(key, quote.json(), ex=self.cache_ttl)
            except Exception as e:
                logger.warning(f"[PricingUtils] Redis set failed: {e}")
        return quote

    async def fetch_batch_prices(self, symbols: List[str]) -> Dict[str, PriceQuote]:
        """Fetch multiple symbols concurrently."""
        tasks = [self.fetch_price(sym) for sym in symbols]
        results: Dict[str, PriceQuote] = {}
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, res in zip(symbols, responses):
            if isinstance(res, PriceQuote):
                results[sym.upper()] = res
            else:
                logger.warning(f"[PricingUtils] Batch fetch error for {sym}: {res}")
        return results

    async def get_historical_prices(self, symbol: str, start: int, end: int) -> List[PriceQuote]:
        """Retrieve historical prices via CoinGecko market chart range API."""
        url = f"{self.gecko_api}/coins/{symbol.lower()}/market_chart/range"
        params = {"vs_currency":"usd", "from":start, "to":end}
        with HTTP_LATENCY.time():
            resp = await self.http.get(url, params=params)
            resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices", [])
        return [PriceQuote(symbol=symbol.upper(), price=p[1], timestamp=int(p[0]/1000)) for p in prices]
