# core/utils/trusted_authority_manager.py

import os
import json
import logging
import asyncio
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import httpx
import redis.asyncio as redis_client
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.trusted_authority_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
AUTHORITY_FETCHES = Counter("authority_fetches_total", "Total fetch requests for authorities")
AUTHORITY_ERRORS = Counter("authority_errors_total", "Total errors during authority fetches")
AUTHORITY_LATENCY = Histogram("authority_latency_seconds", "Latency of authority fetches")
AUTHORITY_CACHES = Counter("authority_cache_hits_total", "Cache hits for trusted authorities")

# ────────────────────────────────────────────────────────────────────────────────
# Configuration & Paths
# ────────────────────────────────────────────────────────────────────────────────
COINGECKO_API_URL = config.GECKO_API_URL.rstrip('/') + "/coins/markets"
HELIUS_METADATA_URL = f"{config.HELIUS_RPC}/tokens/metadata"
OUTPUT_PATH = Path(getattr(config, "TRUSTED_AUTH_OUTPUT", "data/trusted_authorities.json"))

# ────────────────────────────────────────────────────────────────────────────────
# Redis Cache
# ────────────────────────────────────────────────────────────────────────────────
CACHE_KEY = "trusted_authorities"
CACHE_TTL = int(getattr(config, "TRUSTED_AUTH_CACHE_TTL", 86400))  # seconds
redis: Optional[redis_client.Redis] = None

async def _connect_redis() -> None:
    global redis
    if redis is None:
        try:
            redis = redis_client.from_url(config.REDIS_URL, decode_responses=True)
            await redis.ping()
            logger.info("[TrustedAuthority] Connected to Redis")
        except Exception as e:
            redis = None
            logger.warning(f"[TrustedAuthority] Redis unavailable: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Core Functions
# ────────────────────────────────────────────────────────────────────────────────
async def fetch_top_solana_tokens(limit: int = 100) -> List[Dict[str, Any]]:
    """
    Fetch top Solana ecosystem tokens by market cap from CoinGecko.
    """
    AUTHORITY_FETCHES.inc()
    async with AUTHORITY_LATENCY.time():
        try:
            params = {
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": limit,
                "page": 1,
                "sparkline": "false",
                "category": "solana-ecosystem"
            }
            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(COINGECKO_API_URL, params=params)
                resp.raise_for_status()
                return resp.json()
        except Exception as e:
            AUTHORITY_ERRORS.inc()
            logger.error(f"[TrustedAuthority] CoinGecko fetch failed: {e}")
            return []

async def fetch_update_authority(mint: str) -> Optional[str]:
    """
    Fetch the updateAuthority field for a given mint via Helius.
    """
    AUTHORITY_FETCHES.inc()
    async with AUTHORITY_LATENCY.time():
        try:
            payload = {"mintAccounts": [mint]}
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {config.HELIUS_API_KEY}"}
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(HELIUS_METADATA_URL, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
            if isinstance(data, list) and data:
                meta = data[0].get("onChainMetadata", {})
                return meta.get("updateAuthority")
        except Exception as e:
            AUTHORITY_ERRORS.inc()
            logger.error(f"[TrustedAuthority] Helius metadata failed for {mint}: {e}")
        return None

async def refresh_trusted_authorities() -> Set[str]:
    """
    Refresh the trusted authorities list by fetching top tokens and their updateAuthority.
    Caches result in Redis and writes to OUTPUT_PATH.
    """
    await _connect_redis()
    authorities: Set[str] = set()
    tokens = await fetch_top_solana_tokens()
    for token in tokens:
        mint = token.get("platforms", {}).get("solana")
        if not mint:
            continue
        auth = await fetch_update_authority(mint)
        if auth:
            authorities.add(auth)
    # Persist
    try:
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(OUTPUT_PATH, "w") as f:
            json.dump(sorted(authorities), f, indent=2)
    except Exception as e:
        logger.warning(f"[TrustedAuthority] File write failed: {e}")
    # Cache
    if redis:
        try:
            await redis.set(CACHE_KEY, json.dumps(list(authorities)), ex=CACHE_TTL)
        except Exception as e:
            logger.warning(f"[TrustedAuthority] Redis set failed: {e}")
    return authorities

async def load_trusted_authorities() -> Set[str]:
    """
    Load trusted authorities from Redis or fallback to file, or default set.
    """
    await _connect_redis()
    # Try Redis cache
    if redis:
        try:
            raw = await redis.get(CACHE_KEY)
            if raw:
                AUTHORITY_CACHES.inc()
                return set(json.loads(raw))
        except Exception as e:
            logger.warning(f"[TrustedAuthority] Redis get failed: {e}")
    # Fallback file
    if OUTPUT_PATH.exists():
        try:
            with open(OUTPUT_PATH, "r") as f:
                items = json.load(f)
                return set(items)
        except Exception as e:
            logger.warning(f"[TrustedAuthority] File load failed: {e}")
    # Default authorities
    return set(getattr(config, "TRUSTED_AUTHORITIES", []))

async def audit_token(address: str) -> Dict[str, Any]:
    """
    Audit a token's authority against the trusted list.
    """
    AUTHORITY_FETCHES.inc()
    ts = int(datetime.utcnow().timestamp())
    async with AUTHORITY_LATENCY.time():
        trusted = await load_trusted_authorities()
        safe = address in trusted
        return {
            "safe": safe,
            "reason": "Trusted authority" if safe else "Unrecognized authority",
            "timestamp": ts,
        }

# Synchronous entrypoint
def run_authority_refresh_blocking() -> None:
    """Blocking wrapper for refresh_trusted_authorities."""
    asyncio.run(refresh_trusted_authorities())

# Clean up HTTP session if needed
async def close_authority_manager() -> None:
    # No persistent sessions to close
    pass
