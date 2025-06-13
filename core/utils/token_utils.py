import os
import time
import logging
import aiohttp
from typing import Dict, Optional, Union
from dotenv import load_dotenv

load_dotenv()

# ─── Config ─────────────────────────────────────────────────────
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
HELIUS_BASE_URL = "https://api.helius.xyz/v0"
BIRDEYE_PRICE_URL = "https://public-api.birdeye.so/public/price"
HEADERS = {"Content-Type": "application/json"}
CACHE_DURATION = 300  # seconds

# ─── Logger ─────────────────────────────────────────────────────
log = logging.getLogger("token_utils")
logging.basicConfig(level=logging.INFO)

# ─── In-Memory Cache ────────────────────────────────────────────
_cache: Dict[str, Dict[str, Union[float, dict]]] = {}

# ─── Internal GET with Cache ────────────────────────────────────
async def cached_get(url: str, params: Optional[dict] = None) -> dict:
    """
    Fetches data from the given URL and caches it for the specified duration.
    Returns cached data if it exists and is still valid.
    """
    key = f"{url}:{str(params)}"
    now = time.time()

    # Check if cache is available and not expired
    if key in _cache and now - _cache[key]["ts"] < CACHE_DURATION:
        log.info(f"Cache hit for {key}")
        return _cache[key]["data"]

    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get(url, headers=HEADERS, params=params) as resp:
                if resp.status != 200:
                    raise Exception(f"HTTP {resp.status}: {await resp.text()}")
                data = await resp.json()
                _cache[key] = {"ts": now, "data": data}
                log.info(f"Cache miss for {key}, data fetched")
                return data
    except Exception as e:
        log.error(f"[cached_get] Request failed for {url}: {e}")
        return {}

# ─── Get Token Metadata ─────────────────────────────────────────
async def get_token_metadata(mint: str) -> Dict[str, Union[str, int]]:
    """
    Fetches metadata for a token using the Helius API and caches the result.

    :param mint: The mint address of the token.
    :return: A dictionary containing the token's name, symbol, and decimals.
    """
    try:
        url = f"{HELIUS_BASE_URL}/tokens/metadata?api-key={HELIUS_API_KEY}"
        params = {"mintAccounts": [mint]}
        data = await cached_get(url, params)

        meta = data[0] if isinstance(data, list) and data else {}
        return {
            "name": meta.get("name", "Unknown Token"),
            "symbol": meta.get("symbol", "???"),
            "decimals": meta.get("decimals", 6),
        }
    except Exception as e:
        log.error(f"[Token Metadata Error] mint={mint} | {e}")
        return {"name": "Unknown", "symbol": "???", "decimals": 6}

# ─── Get Token Price ────────────────────────────────────────────
async def get_token_price(mint: str) -> float:
    """
    Fetches the current price of a token from Birdeye API and caches the result.

    :param mint: The mint address of the token.
    :return: The token's price or 0.0 if the price cannot be fetched.
    """
    try:
        # Primary source: Birdeye
        params = {"address": mint}
        data = await cached_get(BIRDEYE_PRICE_URL, params)

        price = float(data.get("data", {}).get("value", 0.0))
        if price <= 0:
            raise ValueError("Invalid price received.")

        return price

    except Exception as e:
        log.warning(f"[Token Price Warning] Birdeye failed: {e}")
        return 0.0

# ─── Cache Expiry Management ───────────────────────────────────
def clear_cache() -> None:
    """
    Clears the in-memory cache manually.
    This can be useful for ensuring that the cache is refreshed on demand.
    """
    global _cache
    _cache = {}
    log.info("Cache cleared")

# ─── Cache Metrics ─────────────────────────────────────────────
def cache_metrics() -> Dict[str, int]:
    """
    Returns the current cache statistics, including the number of cache hits and misses.
    """
    cache_hits = sum(1 for key in _cache if time.time() - _cache[key]["ts"] < CACHE_DURATION)
    cache_misses = len(_cache) - cache_hits
    return {"hits": cache_hits, "misses": cache_misses}
