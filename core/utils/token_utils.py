# core/utils/token_utils.py
"""
Elite Token Utilities for Nova Pulse:
- Async metadata & price fetch with retries
- Redis caching with Prometheus metrics
- Robust error handling with fallbacks
"""
import logging
from datetime import datetime
from typing import Optional

import httpx
import redis.asyncio as redis_client
from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_utils")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
CACHE_HITS = Counter(
    "token_utils_cache_hits_total", "Cache hits for token utils"
)
CACHE_MISSES = Counter(
    "token_utils_cache_misses_total", "Cache misses for token utils"
)
TOKEN_ERRORS = Counter(
    "token_utils_errors_total", "Errors in token utils"
)
CACHE_LATENCY = Histogram(
    "token_utils_cache_latency_seconds", "Latency of cache operations"
)
HTTP_LATENCY = Histogram(
    "token_utils_http_latency_seconds", "Latency of HTTP requests"
)

# ────────────────────────────────────────────────────────────────────────────────
# Data Models
# ────────────────────────────────────────────────────────────────────────────────
class TokenMetadata(BaseModel):
    name: str
    symbol: str
    decimals: int = Field(..., ge=0)

class TokenPrice(BaseModel):
    mint: str
    price: float = Field(..., gt=0)
    timestamp: int

# ────────────────────────────────────────────────────────────────────────────────
# Token Utilities
# ────────────────────────────────────────────────────────────────────────────────
class TokenUtils:
    def __init__(self, cache_ttl: int = 300):
        self.redis_url = config.REDIS_URL
        self.helius_key = config.HELIUS_API_KEY
        self.helius_base = config.HELIUS_RPC.rstrip('/')
        self.birdeye_url = config.GECKO_API_URL.rstrip('/') + "/public/price"
        self.cache_ttl = cache_ttl
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=10)

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(
                self.redis_url, decode_responses=True
            )
            await self.redis.ping()
            logger.info("[TokenUtils] Redis connected")
        except Exception as e:
            logger.error(f"[TokenUtils] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        await self.http.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[TokenUtils] Shutdown complete")

    async def get_token_metadata(self, mint: str) -> TokenMetadata:
        key = f"token_meta:{mint}"
        start = datetime.utcnow().timestamp()
        try:
            # Try cache
            with CACHE_LATENCY.time():
                if self.redis:
                    data = await self.redis.get(key)
                    if data:
                        CACHE_HITS.inc()
                        return TokenMetadata.parse_raw(data)
            CACHE_MISSES.inc()

            # Fetch metadata from Helius API
            url = f"{self.helius_base}/tokens/metadata"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.helius_key}"
            }
            payload = {"mintAccounts": [mint]}
            async with HTTP_LATENCY.time():
                resp = await self.http.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                items = resp.json()

            item = items[0] if items else {}
            meta = TokenMetadata(
                name=item.get("name", ""),
                symbol=item.get("symbol", mint[:4].upper()),
                decimals=item.get("decimals", 6),
            )
            # Cache result
            if self.redis:
                await self.redis.set(key, meta.json(), ex=self.cache_ttl)
            return meta
        except Exception as e:
            TOKEN_ERRORS.inc()
            logger.error(f"[TokenUtils] Metadata fetch failed for {mint}: {e}")
            # Fallback metadata
            return TokenMetadata(
                name="Unknown",
                symbol=mint[:4].upper(),
                decimals=6,
            )

    async def get_token_price(self, mint: str) -> TokenPrice:
        key = f"token_price:{mint}"
        try:
            with CACHE_LATENCY.time():
                if self.redis:
                    data = await self.redis.get(key)
                    if data:
                        CACHE_HITS.inc()
                        return TokenPrice.parse_raw(data)
            CACHE_MISSES.inc()

            # Fetch price with retry
            params = {"address": mint}
            async for _ in AsyncRetrying(
                stop=stop_after_attempt(3),
                wait=wait_exponential_jitter(1, 5),
                reraise=True,
            ):
                async with HTTP_LATENCY.time():
                    resp = await self.http.get(self.birdeye_url, params=params)
                    resp.raise_for_status()
                    payload = resp.json()
                    value = payload.get("data", {}).get("value")
                    if not value or value <= 0:
                        raise ValueError("Invalid price data")
                    price = float(value)
                    ts = int(datetime.utcnow().timestamp())
                    quote = TokenPrice(mint=mint, price=price, timestamp=ts)
                    if self.redis:
                        await self.redis.set(key, quote.json(), ex=self.cache_ttl)
                    return quote
        except Exception as e:
            TOKEN_ERRORS.inc()
            logger.error(f"[TokenUtils] Price fetch failed for {mint}: {e}")
            # Fallback zero price
            return TokenPrice(
                mint=mint,
                price=0.0,
                timestamp=int(datetime.utcnow().timestamp()),
            )
