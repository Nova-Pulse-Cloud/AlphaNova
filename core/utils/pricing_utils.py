import asyncio
import logging
import json
from typing import Dict, List
import aioredis
import httpx
from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from utils.config import Config

logger = logging.getLogger("pricing_utils")

# === Data Models ===
class PriceQuote(BaseModel):
    symbol: str
    price: float = Field(..., gt=0)
    timestamp: int

# === Pricing Client ===
class PricingUtils:
    def __init__(self, cache_ttl: int = 5):
        """
        Initialize PricingUtils with cache and API details.
        
        :param cache_ttl: Time-to-live for cached prices in Redis (default: 5 seconds).
        """
        self.redis_url = Config.REDIS_URL
        self.cmc_api_key = Config.CMC_API_KEY
        self.cache_ttl = cache_ttl
        self.redis = None
        self.http_client = httpx.AsyncClient(timeout=10)

    async def connect(self):
        """Connect to Redis and set up HTTP client."""
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            logger.info("[PricingUtils] Connected to Redis cache.")
        except Exception as e:
            logger.error(f"[PricingUtils] Redis connection error: {e}")
            raise

    async def disconnect(self):
        """Disconnect from Redis and HTTP client."""
        await self.http_client.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[PricingUtils] Shutdown complete.")

    async def fetch_price(self, symbol: str) -> PriceQuote:
        """
        Fetch the current price for a symbol. First checks Redis, then fetches from API if not cached.
        
        :param symbol: The symbol for which to fetch the price (e.g., 'BTC').
        :return: A PriceQuote object with the symbol, price, and timestamp.
        """
        key = f"price:{symbol.upper()}"
        try:
            # Try fetching price from Redis cache first
            cached = await self.redis.get(key)
            if cached:
                return PriceQuote.parse_raw(cached)

            # If not in cache, fetch price from CoinMarketCap API
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
            headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key}
            params = {"symbol": symbol.upper()}

            # Retry logic with exponential backoff
            for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 5), reraise=True):
                with attempt:
                    response = await self.http_client.get(url, headers=headers, params=params)
                    response.raise_for_status()
                    data = response.json()
                    price = data["data"][symbol.upper()]["quote"]["USD"]["price"]

                    # Store the fetched price in Redis cache
                    quote = PriceQuote(symbol=symbol.upper(), price=price, timestamp=int(asyncio.get_event_loop().time()))
                    await self.redis.set(key, quote.json(), ex=self.cache_ttl)
                    return quote

        except Exception as e:
            logger.error(f"[Pricing] Failed to fetch price for {symbol}: {e}")
            raise

    async def fetch_batch_prices(self, symbols: List[str]) -> Dict[str, PriceQuote]:
        """
        Fetch the prices for multiple symbols concurrently.

        :param symbols: A list of symbols to fetch prices for (e.g., ['BTC', 'ETH']).
        :return: A dictionary of PriceQuote objects for each symbol.
        """
        results: Dict[str, PriceQuote] = {}
        tasks = [self.fetch_price(sym) for sym in symbols]
        
        # Gather all the tasks concurrently
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        for sym, res in zip(symbols, responses):
            if isinstance(res, PriceQuote):
                results[sym.upper()] = res
            else:
                logger.warning(f"[Pricing] Skipped {sym}: {res}")
        
        return results

    async def get_historical_prices(self, symbol: str, start: int, end: int) -> List[PriceQuote]:
        """
        Fetch historical prices for a symbol between start and end timestamps.
        This method is intentionally stubbed for now, and can be expanded later.

        :param symbol: The symbol to fetch historical prices for.
        :param start: The start timestamp.
        :param end: The end timestamp.
        :return: A list of PriceQuote objects for the historical prices.
        """
        logger.warning("[Pricing] get_historical_prices is not implemented: no backfill or synthetic data.")
        return []

# === Notes ===
# The get_historical_prices method is intentionally stubbed, as it requires access to a historical price feed.
# Future upgrade path: integrate with an on-chain oracle or paid historical price feed for backfill data.
# The batch fetching of prices is safe for large-scale environments and supports retries with backoff.
