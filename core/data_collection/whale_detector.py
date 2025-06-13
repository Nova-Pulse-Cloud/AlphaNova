import os
import logging
import aiohttp
import time
from datetime import datetime
from typing import List, Dict, Any
from prometheus_client import Counter, Histogram

logger = logging.getLogger("whale_detector")

# === Prometheus metrics ===
WHALE_DETECTION_CALLS = Counter("whale_detection_calls_total", "Total whale detection API calls")
WHALE_DETECTION_ERRORS = Counter("whale_detection_errors_total", "Total errors in whale detection")
WHALE_DETECTION_LATENCY = Histogram("whale_detection_latency_seconds", "Latency of whale detection")
WHALER_DETECTION_CACHE_HITS = Counter("whale_detection_cache_hits_total", "Cache hits for whale detection")
WHALER_DETECTION_CACHE_MISSES = Counter("whale_detection_cache_misses_total", "Cache misses for whale detection")

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
SOLANA_ADDRESS_BOOK = os.getenv("SOLANA_ADDRESS_BOOK", "https://api.helius.xyz/v0/addresses")  # Example
SUI_API_URL = os.getenv("SUI_API_URL")  # For Sui whale scanning

# ---- Caching mechanism (TTL 5 minutes) ----
cache: Dict[str, Dict[str, Any]] = {}
cache_ttl = 300  # 5 minutes in seconds

async def get_whale_activity_from_cache(symbol: str, chain: str) -> Dict[str, Any]:
    key = f"{symbol}-{chain}"
    current_time = time.time()
    cached_data = cache.get(key)

    if cached_data and (current_time - cached_data['timestamp'] < cache_ttl):
        WHALER_DETECTION_CACHE_HITS.inc()
        return cached_data['data']
    else:
        WHALER_DETECTION_CACHE_MISSES.inc()
        return None

async def set_whale_activity_cache(symbol: str, chain: str, data: Dict[str, Any]):
    key = f"{symbol}-{chain}"
    cache[key] = {
        "timestamp": time.time(),
        "data": data
    }

# === Whale Activity Detection ===
async def get_recent_whale_activity(symbol: str, chain: str = "solana") -> dict:
    """
    Get live recent whale trades/swaps for a given token symbol on supported chain.
    Supports Solana (Helius), Sui (add your own endpoint), and plug-in for more.
    Returns standardized whale meta for AI/risk/rotation/Telegram.
    """
    start_time = time.monotonic()
    try:
        whales = []
        score = 0
        
        # Check cache first
        cached_data = await get_whale_activity_from_cache(symbol, chain)
        if cached_data:
            return cached_data
        
        if chain == "solana" and HELIUS_API_KEY:
            url = f"https://api.helius.xyz/v0/token-transfers?api-key={HELIUS_API_KEY}&symbol={symbol}&limit=10"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as res:
                    if res.status == 200:
                        data = await res.json()
                        for tx in data.get("transfers", []):
                            # Simplified: check for big swaps/mints/burns
                            if tx.get("amount", 0) > 10000:  # Amount threshold for whale activity
                                whales.append({
                                    "from": tx.get("from"),
                                    "to": tx.get("to"),
                                    "amount": tx.get("amount"),
                                    "tx": tx.get("signature"),
                                    "time": tx.get("blockTime")
                                })
                        score = min(len(whales) / 10, 1.0)  # Simple score based on whale count

        # (Future: Add Sui, ETH, BSC, and other chains by plug-in)
        result = {
            "whales": whales,
            "score": score,
            "last_check": datetime.utcnow().isoformat(),
        }

        # Set the cache for future requests
        await set_whale_activity_cache(symbol, chain, result)
        
        WHALE_DETECTION_LATENCY.observe(time.monotonic() - start_time)
        return result

    except Exception as e:
        WHALE_DETECTION_ERRORS.inc()
        logger.error(f"[WhaleDetector] Error for {symbol} on {chain}: {e}")
        return {"whales": [], "score": 0, "error": str(e)}

# === Retrieve Tokens with Recent Whale Activity ===
async def get_recent_whale_tokens(limit: int = 5, chain: str = "solana") -> str:
    """
    Get a list of top tokens with the most recent whale activity—demo-ready for Telegram and dashboard.
    """
    try:
        results = []
        symbols = ["SOL", "USDC", "BONK", "RAY", "JUP"]  # Add top tokens from your source
        for symbol in symbols[:limit]:
            meta = await get_recent_whale_activity(symbol, chain=chain)
            if meta.get("score", 0) > 0:
                results.append(f"{symbol} (whale score: {meta['score']:.2f})")
        
        return "\n".join(results) if results else "No recent whale activity found."
    
    except Exception as e:
        WHALE_DETECTION_ERRORS.inc()
        logger.error(f"[WhaleDetector] Whale tokens error: {e}")
        return "No whale data."
