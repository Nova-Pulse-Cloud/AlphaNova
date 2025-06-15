# core/utils/pricing_utils.py

import logging
from datetime import datetime
from typing import Dict, List

import redis.asyncio as redis_client
import httpx
from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter

from core.utils.config import config

logger = logging.getLogger("nova.pricing_utils")
logger.setLevel(logging.INFO)

class PriceQuote(BaseModel):
    symbol: str
    price: float = Field(..., gt=0)
    timestamp: int

class PricingUtils:
    """
    Production-grade pricing utility:
    - Caches via Redis
    - Fetches from CoinMarketCap; falls back to CoinGecko
    - Handles retries, TTLs, and historical data via Gecko
    """
    def __init__(self, cache_ttl: int = 5):
        self.redis_url = config.REDIS_URL
        self.cmc_api_key = config.CMC_API_KEY
        self.gecko_api_url = config.GECKO_API_URL
        self.cache_ttl = cache_ttl
        self.redis: redis_client.Redis | None = None
        self.http_client = httpx.AsyncClient(timeout=10)

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[PricingUtils] Connected to Redis")
        except Exception as e:
            logger.error(f"[PricingUtils] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        await self.http_client.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[PricingUtils] Shutdown complete")

    async def fetch_price(self, symbol: str) -> PriceQuote:
        key = f"price:{symbol.upper()}"
        # Check cache
        if self.redis:
            try:
                cached = await self.redis.get(key)
                if cached:
                    return PriceQuote.parse_raw(cached)
            except Exception as e:
                logger.warning(f"[PricingUtils] Redis get failed: {e}")
        # Primary: CoinMarketCap API
        cmc_url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest"
        headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key}
        params = {"symbol": symbol.upper()}
        for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 5), reraise=True):
            with attempt:
                resp = await self.http_client.get(cmc_url, headers=headers, params=params)
                resp.raise_for_status()
                data = resp.json()
                price = data["data"][symbol.upper()]["quote"]["USD"]["price"]
                ts = int(datetime.utcnow().timestamp())
                quote = PriceQuote(symbol=symbol.upper(), price=price, timestamp=ts)
                if self.redis:
                    try:
                        await self.redis.set(key, quote.json(), ex=self.cache_ttl)
                    except Exception as e:
                        logger.warning(f"[PricingUtils] Redis set failed: {e}")
                return quote
        # Fallback: CoinGecko API
        gecko_key = f"gecko:price:{symbol.lower()}"
        for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 5), reraise=True):
            with attempt:
                resp = await self.http_client.get(f"{self.gecko_api_url}/simple/price", params={"ids": symbol.lower(), "vs_currencies": "usd"})
                resp.raise_for_status()
                data = resp.json()
                price = data.get(symbol.lower(), {}).get("usd")
                if price is None:
                    raise RuntimeError(f"No price for {symbol} from Gecko")
                ts = int(datetime.utcnow().timestamp())
                quote = PriceQuote(symbol=symbol.upper(), price=price, timestamp=ts)
                if self.redis:
                    try:
                        await self.redis.set(key, quote.json(), ex=self.cache_ttl)
                    except Exception as e:
                        logger.warning(f"[PricingUtils] Redis set failed: {e}")
                return quote
        raise RuntimeError(f"Failed to fetch price for {symbol}")

    async def fetch_batch_prices(self, symbols: List[str]) -> Dict[str, PriceQuote]:
        tasks = [self.fetch_price(sym) for sym in symbols]
        results: Dict[str, PriceQuote] = {}
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        for sym, res in zip(symbols, responses):
            if isinstance(res, PriceQuote):
                results[sym.upper()] = res
            else:
                logger.warning(f"[PricingUtils] Fetch failed for {sym}: {res}")
        return results

    async def get_historical_prices(self, symbol: str, start: int, end: int) -> List[PriceQuote]:
        """
        Retrieve historical price data from CoinGecko between UNIX timestamps start and end.
        Symbol must be a CoinGecko coin ID (e.g., 'solana').
        """
        url = f"{self.gecko_api_url}/coins/{symbol.lower()}/market_chart/range"
        params = {"vs_currency": "usd", "from": start, "to": end}
        resp = await self.http_client.get(url, params=params)
        resp.raise_for_status()
        data = resp.json()
        prices = data.get("prices", [])
        return [PriceQuote(symbol=symbol.upper(), price=p[1], timestamp=int(p[0] / 1000)) for p in prices]
