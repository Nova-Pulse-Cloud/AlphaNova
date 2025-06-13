import asyncio
import logging
import time
from typing import List, Dict, Any, Optional

import aiohttp
import httpx
import aioredis
from pydantic import BaseModel
from prometheus_client import Counter, Histogram
from utils.config import Config

logger = logging.getLogger("token_scanner")

# ─── Prometheus Metrics ──────────────────────────────────────────
token_scan_requests = Counter("token_scanner_requests_total", "Total token scan requests")
token_scan_latency = Histogram("token_scanner_latency_seconds", "Latency of token scan operations")
token_scan_errors = Counter("token_scanner_errors_total", "Total errors in token scanning")

# ─── Data Model ─────────────────────────────────────────────────
class TokenStats(BaseModel):
    symbol: str
    chain: str
    mint: str
    total_supply: float
    holder_count: int
    volume_24h: float
    timestamp: int
    whale_flag: Optional[bool] = False
    social_viral_score: Optional[float] = None
    rug_flag: Optional[bool] = False

class TokenScanner:
    def __init__(self, redis_url: str, cmc_api_key: str, redis_ttl: int = 86400):
        """
        Initializes the TokenScanner with configuration settings.

        :param redis_url: URL for the Redis cache.
        :param cmc_api_key: CoinMarketCap API key.
        :param redis_ttl: Cache expiration time (default: 86400 seconds).
        """
        self.redis_url = redis_url
        self.cmc_api_key = cmc_api_key
        self.redis = None
        self.http = httpx.AsyncClient()
        self.redis_ttl = redis_ttl

    async def connect(self):
        """Connects to Redis."""
        if not self.redis:
            self.redis = await aioredis.from_url(self.redis_url)
            logger.info("TokenScanner connected to Redis.")

    async def disconnect(self):
        """Disconnects from Redis and HTTP client."""
        await self.http.aclose()
        if self.redis:
            await self.redis.close()

    async def get_all_tokens(self, chain: str = "solana", filter_rugs: bool = True) -> List[TokenStats]:
        """
        Retrieves all tokens from the specified chain and applies various overlays like whale tracking, social scores, and rug checks.
        
        :param chain: The blockchain (e.g., "solana").
        :param filter_rugs: Whether to filter out rug-pulled tokens.
        :return: A list of TokenStats with additional metadata like whale flags, social viral score, and rug flags.
        """
        token_scan_requests.inc()
        async with token_scan_latency.time():
            await self.connect()
            cache_key = f"token_list:{chain}"
            cached = await self.redis.get(cache_key)
            if cached:
                logger.info(f"Cache hit for token list from chain: {chain}")
                return [TokenStats.parse_raw(item) for item in eval(cached)]

            tokens = await self._fetch_tokens_dexscreener(chain)
            # Apply overlays (whale, social, rug)
            whale_tokens = await self._whale_overlay(tokens, chain)
            viral_scores = await self._social_overlay([t['mint'] for t in tokens])
            for t in tokens:
                t['whale_flag'] = t['mint'] in whale_tokens
                t['social_viral_score'] = viral_scores.get(t['mint'], 0)
                t['rug_flag'] = await self._is_rug(t['mint']) if filter_rugs else False

            # Filter out rug-pulled tokens
            token_stats = [TokenStats(**t) for t in tokens if not t.get('rug_flag', False)]
            await self.redis.set(cache_key, str([t.json() for t in token_stats]), ex=self.redis_ttl)
            return token_stats

    async def _fetch_tokens_dexscreener(self, chain: str) -> List[Dict[str, Any]]:
        """
        Fetches token data from DEXScreener API for a specific blockchain.
        
        :param chain: The blockchain (e.g., "solana").
        :return: A list of token metadata.
        """
        url = f"https://api.dexscreener.com/latest/dex/pairs/{chain}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=12) as resp:
                    data = await resp.json()
                    tokens = []
                    for pair in data.get("pairs", []):
                        base = pair.get("baseToken", {})
                        tokens.append({
                            "symbol": base.get("symbol", base.get("address")[:4]),
                            "chain": chain,
                            "mint": base.get("address"),
                            "total_supply": float(pair.get("liquidity", 0)),
                            "holder_count": int(pair.get("txCount", 0)),
                            "volume_24h": float(pair.get("volume", 0)),
                            "timestamp": int(time.time()),
                        })
                    return tokens
        except Exception as e:
            token_scan_errors.inc()
            logger.warning(f"[TokenScanner] DEX fetch error: {e}")
            return []

    async def _whale_overlay(self, tokens: List[Dict], chain: str) -> List[str]:
        """
        Flags tokens bought by top wallets within the last N minutes. This function is currently a placeholder and can be extended to integrate with real whale detection.
        
        :param tokens: List of token metadata.
        :param chain: The blockchain (e.g., "solana").
        :return: A list of token mints identified as being bought by whales.
        """
        # For demo purposes, we return an empty list. This should integrate with real whale detection logic.
        return []

    async def _social_overlay(self, mints: List[str]) -> Dict[str, float]:
        """
        Scans social platforms (e.g., X, Telegram, Reddit) for viral token mentions. This function is a placeholder for future integration.
        
        :param mints: List of token mints to scan.
        :return: A dictionary of token mints and their associated social media viral score.
        """
        # For demo purposes, all tokens are assigned a score of 0.
        return {mint: 0.0 for mint in mints}

    async def _is_rug(self, mint: str) -> bool:
        """
        Determines if a token is a potential rug-pull using core risk signals or known rug lists.
        
        :param mint: The token's mint address.
        :return: True if the token is flagged as a rug, False otherwise.
        """
        # This function should be connected to a live risk signal module.
        # For demo purposes, it always returns False.
        return False

    async def get_token_by_mint(self, mint: str, chain: str = "solana") -> Optional[TokenStats]:
        """
        Retrieves a token's details based on its mint address.

        :param mint: The mint address of the token.
        :param chain: The blockchain (default: "solana").
        :return: A TokenStats object or None if the token is not found.
        """
        tokens = await self.get_all_tokens(chain)
        for token in tokens:
            if token.mint == mint:
                return token
        return None
