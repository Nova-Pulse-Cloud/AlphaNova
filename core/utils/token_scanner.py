# core/utils/token_scanner.py

import time
import json
import logging
from typing import List, Dict, Any, Optional

import httpx
import redis.asyncio as redis_client
from prometheus_client import Counter, Histogram
from pydantic import BaseModel

from core.utils.config import config
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.social_sentiment import score_token_sentiment
from core.utils.risk_signals import is_rug

logger = logging.getLogger("nova.token_scanner")
logger.setLevel(logging.INFO)

# Prometheus metrics
TOKEN_SCAN_REQUESTS = Counter(
    "token_scanner_requests_total", "Total token scan requests"
)
TOKEN_SCAN_LATENCY = Histogram(
    "token_scanner_latency_seconds", "Latency of token scan operations"
)
TOKEN_SCAN_ERRORS = Counter(
    "token_scanner_errors_total", "Total errors in token scanning"
)

class TokenStats(BaseModel):
    symbol: str
    chain: str
    mint: str
    total_supply: float
    holder_count: int
    volume_24h: float
    timestamp: int
    whale_flag: bool = False
    social_score: float = 0.0
    rug_flag: bool = False

class TokenScanner:
    """
    Scans tokens on a blockchain via Dexscreener, applies whale, social, and rug overlays,
    and caches results in Redis for high-performance reuse.
    """
    def __init__(self, chain: str = "solana", cache_ttl: int = 86400) -> None:
        self.chain = chain
        self.cache_ttl = cache_ttl
        self.redis_url = config.REDIS_URL
        self.http = httpx.AsyncClient(timeout=15)
        self.redis: Optional[redis_client.Redis] = None
        self.dexscreener_url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}"

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TokenScanner] Connected to Redis")
        except Exception as e:
            logger.error(f"[TokenScanner] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        """Close HTTP and Redis connections."""
        await self.http.aclose()
        if self.redis:
            await self.redis.close()

    async def get_all_tokens(self, filter_rugs: bool = True) -> List[TokenStats]:
        """
        Retrieve token stats, apply overlays, and cache results.
        """
        TOKEN_SCAN_REQUESTS.inc()
        start = time.time()
        try:
            await self.connect()
            cache_key = f"token_list:{self.chain}"
            # Return cached if available
            if self.redis:
                raw = await self.redis.get(cache_key)
                if raw:
                    stats = [TokenStats.parse_raw(item) for item in json.loads(raw)]
                    TOKEN_SCAN_LATENCY.observe(time.time() - start)
                    return stats

            # Fetch fresh token data
            tokens_data = await self._fetch_tokens_dexscreener()
            mints = [t["mint"] for t in tokens_data]
            whales = get_recent_whale_activity(self.chain, mints)
            social_scores = {mint: score_token_sentiment(mint) for mint in mints}

            results: List[TokenStats] = []
            for t in tokens_data:
                mint = t["mint"]
                stat = TokenStats(
                    symbol=t["symbol"],
                    chain=self.chain,
                    mint=mint,
                    total_supply=t["total_supply"],
                    holder_count=t["holder_count"],
                    volume_24h=t["volume_24h"],
                    timestamp=int(time.time()),
                    whale_flag=mint in whales,
                    social_score=social_scores.get(mint, 0.0),
                    rug_flag=is_rug(mint) if filter_rugs else False,
                )
                if not stat.rug_flag:
                    results.append(stat)

            # Cache for future
            if self.redis:
                await self.redis.set(
                    cache_key,
                    json.dumps([r.json() for r in results]),
                    ex=self.cache_ttl,
                )

            TOKEN_SCAN_LATENCY.observe(time.time() - start)
            return results
        except Exception as e:
            TOKEN_SCAN_ERRORS.inc()
            logger.error(f"[TokenScanner] get_all_tokens error: {e}")
            raise

    async def _fetch_tokens_dexscreener(self) -> List[Dict[str, Any]]:
        """Fetch raw token data from Dexscreener API."""
        try:
            resp = await self.http.get(self.dexscreener_url)
            resp.raise_for_status()
            data = resp.json()
            tokens: List[Dict[str, Any]] = []
            for pair in data.get("pairs", []):
                base = pair.get("baseToken", {})
                tokens.append({
                    "symbol": base.get("symbol", base.get("address")[:4].upper()),
                    "mint": base.get("address"),
                    "total_supply": float(pair.get("liquidity", 0)),
                    "holder_count": int(pair.get("txCount", 0)),
                    "volume_24h": float(pair.get("volume", 0)),
                })
            return tokens
        except Exception as e:
            TOKEN_SCAN_ERRORS.inc()
            logger.error(f"[TokenScanner] _fetch_tokens_dexscreener error: {e}")
            return []
