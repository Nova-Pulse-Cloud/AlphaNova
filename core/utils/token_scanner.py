# core/utils/token_scanner.py

import asyncio
import time
import json
import logging
from typing import List, Dict, Any, Optional, Tuple

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

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TOKEN_SCAN_REQUESTS = Counter("token_scan_requests_total", "Total token scan requests")
TOKEN_SCAN_ERRORS   = Counter("token_scan_errors_total",   "Errors during token scanning")
CACHE_HITS          = Counter("token_scan_cache_hits_total",   "Cache hits for token scans")
CACHE_MISSES        = Counter("token_scan_cache_misses_total", "Cache misses for token scans")
SCAN_LATENCY        = Histogram("token_scan_latency_seconds",   "Latency of token scan operations")

# ────────────────────────────────────────────────────────────────────────────────
# Data Model
# ────────────────────────────────────────────────────────────────────────────────
class TokenStats(BaseModel):
    symbol: str
    chain: str
    mint: str
    total_supply: float
    holder_count: int
    volume_24h: float
    timestamp: int
    whale_flag: bool
    social_score: float
    rug_flag: bool

# ────────────────────────────────────────────────────────────────────────────────
# Token Scanner
# ────────────────────────────────────────────────────────────────────────────────
class TokenScanner:
    """
    Ultra-optimized token scanner:
    - Async bulk overlays (whale, social, rug checks)
    - Redis caching with hit/miss metrics
    - DexScreener integration
    - Error isolation and partial results
    """
    def __init__(self, chain: str = "solana", cache_ttl: int = 86400) -> None:
        self.chain = chain
        self.cache_ttl = cache_ttl
        self.redis_url = config.REDIS_URL
        self.http = httpx.AsyncClient(timeout=15)
        self.redis: Optional[redis_client.Redis] = None
        self.dexscreener_url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}"

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TokenScanner] Connected to Redis")
        except Exception as e:
            logger.error(f"[TokenScanner] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        await self.http.aclose()
        if self.redis:
            await self.redis.close()

    async def get_all_tokens(self, filter_rugs: bool = True) -> List[TokenStats]:
        TOKEN_SCAN_REQUESTS.inc()
        start = time.time()
        await self.connect()
        cache_key = f"token_list:{self.chain}"
        # Cache check
        if self.redis:
            try:
                raw = await self.redis.get(cache_key)
                if raw:
                    CACHE_HITS.inc()
                    stats = [TokenStats.parse_raw(item) for item in json.loads(raw)]
                    SCAN_LATENCY.observe(time.time() - start)
                    return stats
            except Exception as e:
                logger.warning(f"[TokenScanner] Cache get failed: {e}")
        CACHE_MISSES.inc()

        # Fetch base token data
        base_tokens = await self._fetch_tokens_dexscreener()
        mints = [t['mint'] for t in base_tokens]

        # Bulk overlay tasks
        whale_task  = asyncio.create_task(get_recent_whale_activity(self.chain, mints))
        social_task = asyncio.create_task(self._bulk_social_scores(mints))
        rug_task    = asyncio.create_task(self._bulk_rug_flags(mints, filter_rugs))
        whales, socials, rugs = await asyncio.gather(whale_task, social_task, rug_task)

        results: List[TokenStats] = []
        timestamp = int(time.time())
        for token in base_tokens:
            mint = token['mint']
            try:
                stat = TokenStats(
                    symbol=token['symbol'],
                    chain=self.chain,
                    mint=mint,
                    total_supply=token['total_supply'],
                    holder_count=token['holder_count'],
                    volume_24h=token['volume_24h'],
                    timestamp=timestamp,
                    whale_flag = mint in whales,
                    social_score = socials.get(mint, 0.0),
                    rug_flag = rugs.get(mint, False),
                )
                if not stat.rug_flag:
                    results.append(stat)
            except Exception as e:
                logger.warning(f"[TokenScanner] Failed building TokenStats for {mint}: {e}")

        # Cache fresh results
        if self.redis:
            try:
                await self.redis.set(cache_key, json.dumps([r.json() for r in results]), ex=self.cache_ttl)
            except Exception as e:
                logger.warning(f"[TokenScanner] Cache set failed: {e}")

        SCAN_LATENCY.observe(time.time() - start)
        return results

    async def _fetch_tokens_dexscreener(self) -> List[Dict[str, Any]]:
        try:
            resp = await self.http.get(self.dexscreener_url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            TOKEN_SCAN_ERRORS.inc()
            logger.error(f"[TokenScanner] DexScreener fetch error: {e}")
            return []
        tokens: List[Dict[str, Any]] = []
        for pair in data.get('pairs', []):
            base = pair.get('baseToken', {})
            tokens.append({
                'symbol': base.get('symbol', base.get('address')[:4].upper()),
                'mint': base.get('address'),
                'total_supply': float(pair.get('liquidity', 0)),
                'holder_count': int(pair.get('txCount', 0)),
                'volume_24h': float(pair.get('volume', 0)),
            })
        return tokens

    async def _bulk_social_scores(self, mints: List[str]) -> Dict[str, float]:
        scores: Dict[str, float] = {}
        tasks = [asyncio.create_task(score_token_sentiment(m)) for m in mints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for mint, res in zip(mints, results):
            if isinstance(res, Exception):
                logger.warning(f"[TokenScanner] Social score failed for {mint}: {res}")
                scores[mint] = 0.0
            else:
                scores[mint] = res
        return scores

    async def _bulk_rug_flags(self, mints: List[str], filter_rugs: bool) -> Dict[str, bool]:
        flags: Dict[str, bool] = {}
        if not filter_rugs:
            return {m: False for m in mints}
        tasks = [asyncio.create_task(self._is_rug(m)) for m in mints]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for mint, res in zip(mints, results):
            if isinstance(res, Exception):
                logger.warning(f"[TokenScanner] Rug check failed for {mint}: {res}")
                flags[mint] = False
            else:
                flags[mint] = res
        return flags

    async def _is_rug(self, mint: str) -> bool:
        # risk_signals.is_rug may be sync or async
        try:
            if asyncio.iscoroutinefunction(is_rug):
                return await is_rug(mint)
            else:
                loop = asyncio.get_running_loop()
                return await loop.run_in_executor(None, is_rug, mint)
        except Exception as e:
            logger.warning(f"[TokenScanner] is_rug exception for {mint}: {e}")
            return False
