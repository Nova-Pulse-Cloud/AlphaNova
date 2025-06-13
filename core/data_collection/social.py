# social/social.py (elite version)

import aiohttp
import os
import logging
from typing import List, Dict, Tuple, Optional
from prometheus_client import Counter, Histogram

TWITTER_BEARER = os.getenv("TWITTER_BEARER_TOKEN")
JUPITER_TOKENS_URL = "https://quote-api.jup.ag/v6/tokens"

logger = logging.getLogger("social")

# Prometheus metrics
SOCIAL_TOKEN_MENTIONS = Counter("social_token_mentions_total", "Total token mentions fetched")
SOCIAL_API_ERRORS = Counter("social_api_errors_total", "Social API errors (Twitter/Jupiter)")
SOCIAL_API_LATENCY = Histogram("social_api_latency_seconds", "Latency of social fetch calls")

async def fetch_token_metadata() -> Dict[str, str]:
    try:
        async with SOCIAL_API_LATENCY.time():
            async with aiohttp.ClientSession() as session:
                async with session.get(JUPITER_TOKENS_URL, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"[Jupiter API Error] Status: {resp.status}")
                        SOCIAL_API_ERRORS.inc()
                        return {}
                    data = await resp.json()
                    return {token["symbol"].upper(): token["address"] for token in data}
    except Exception as e:
        logger.error(f"[Jupiter Fetch Error] {e}")
        SOCIAL_API_ERRORS.inc()
        return {}

async def get_social_mentions(top_n: int = 10) -> List[Dict]:
    if not TWITTER_BEARER:
        logger.error("[Twitter Config Error] Missing TWITTER_BEARER_TOKEN in environment.")
        return []

    headers = {"Authorization": f"Bearer {TWITTER_BEARER}"}
    query = "(SOL OR memecoin OR pump) lang:en -is:retweet"
    url = (
        "https://api.twitter.com/2/tweets/search/recent?"
        f"query={query}&max_results=100&tweet.fields=created_at"
    )

    try:
        async with SOCIAL_API_LATENCY.time():
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=10) as resp:
                    if resp.status != 200:
                        logger.error(f"[Twitter API Error] Status: {resp.status}")
                        SOCIAL_API_ERRORS.inc()
                        return []
                    data = await resp.json()
    except Exception as e:
        logger.error(f"[Twitter Fetch Error] {e}")
        SOCIAL_API_ERRORS.inc()
        return []

    mentions = {}
    for tweet in data.get("data", []):
        text = tweet.get("text", "").upper()
        # Enhanced token extraction for real-world tokens (e.g. $WIF, $SOL)
        words = {
            w.strip() for w in text.replace("$", " ").split()
            if 2 < len(w) <= 6 and w.isalpha()
        }
        for word in words:
            mentions[word] = mentions.get(word, 0) + 1

    sorted_tokens = sorted(mentions.items(), key=lambda x: -x[1])[:top_n]
    token_map = await fetch_token_metadata()

    results = []
    for sym, count in sorted_tokens:
        results.append({
            "symbol": sym,
            "mint": token_map.get(sym, f"UNKNOWN_{sym}"),
            "mentions": count,
            "velocity": count / 5.0,
        })
        SOCIAL_TOKEN_MENTIONS.inc()
    return results

# === Potential extension: Async generator for streaming mentions, or Redis pubsub ===
