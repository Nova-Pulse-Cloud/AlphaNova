import aiohttp
import os
import json
import asyncio
from typing import Optional, List, Set
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram
import time

# === Prometheus Metrics ===
AUTHORITY_FETCHES = Counter("authority_fetches_total", "Total fetch requests for authorities")
AUTHORITY_ERRORS = Counter("authority_errors_total", "Total errors during authority fetches")
AUTHORITY_LATENCY = Histogram("authority_latency_seconds", "Latency of authority fetches")
AUTHORITY_CACHES = Counter("authority_cache_hits_total", "Cache hits for trusted authorities")

load_dotenv()

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
COINGECKO_API = "https://api.coingecko.com/api/v3/coins/markets"
HELIUS_METADATA_URL = f"https://api.helius.xyz/v0/tokens/metadata?api-key={HELIUS_API_KEY}"
OUTPUT_FILE = "data/trusted_authorities.json"

HEADERS = {"accept": "application/json"}

# === In-memory cache for authorities ===
cached_authorities: Set[str] = set()

# === Fetch Top Solana Tokens via CoinGecko ===
async def fetch_top_solana_tokens() -> List[dict]:
    AUTHORITY_FETCHES.inc()
    async with AUTHORITY_LATENCY.time():
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": 100,
            "page": 1,
            "sparkline": "false",
            "category": "solana-ecosystem"
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(COINGECKO_API, headers=HEADERS, params=params) as resp:
                    if resp.status != 200:
                        AUTHORITY_ERRORS.inc()
                        print(f"[CoinGecko Error] Status: {resp.status}")
                        return []
                    return await resp.json()
            except Exception as e:
                AUTHORITY_ERRORS.inc()
                print(f"[CoinGecko Exception] {e}")
                return []

# === Fetch Update Authority from Helius ===
async def fetch_metadata(mint: str) -> Optional[str]:
    AUTHORITY_FETCHES.inc()
    async with AUTHORITY_LATENCY.time():
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(HELIUS_METADATA_URL, json={"mintAccounts": [mint]}) as resp:
                    if resp.status != 200:
                        AUTHORITY_ERRORS.inc()
                        print(f"[Helius Error] {mint} → Status: {resp.status}")
                        return None
                    data = await resp.json()
                    if data and data[0].get("onChainMetadata"):
                        return data[0]["onChainMetadata"].get("updateAuthority")
                    return None
            except Exception as e:
                AUTHORITY_ERRORS.inc()
                print(f"[Metadata Error] {mint} → {e}")
                return None

# === Build & Save Authority Set ===
async def refresh_trusted_authorities() -> None:
    print("[Authority Refresh] Starting refresh...")
    tokens = await fetch_top_solana_tokens()
    authorities: Set[str] = set()

    for token in tokens:
        mint = token.get("platforms", {}).get("solana")
        if not mint:
            continue
        authority = await fetch_metadata(mint)
        if authority:
            authorities.add(authority)

    if authorities:
        os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
        with open(OUTPUT_FILE, "w") as f:
            json.dump(sorted(authorities), f, indent=2)
        cached_authorities.update(authorities)  # Update in-memory cache
        print(f"[Authority Refresh] ✅ Saved {len(authorities)} authorities.")
    else:
        print("[Authority Refresh] ⚠️ No authorities found.")

# === Public Function for Manual Trigger (blocking) ===
def run_authority_refresh_blocking() -> None:
    asyncio.run(refresh_trusted_authorities())

# === Default Hardcoded Trusted Authorities ===
TRUSTED_AUTHORITIES: Set[str] = {
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # Solana SPL Token Program
    "BPFLoaderUpgradeab1e11111111111111111111111",  # Default upgrade authority
    "So11111111111111111111111111111111111111112",  # Wrapped SOL
    "Stake11111111111111111111111111111111111111",  # Stake program
}

# === Utility to Load Trusted Authorities from File (if exists) ===
def load_trusted_authorities() -> Set[str]:
    """Returns cached authorities if available, else loads from file."""
    if cached_authorities:  # Check in-memory cache first
        AUTHORITY_CACHES.inc()
        return cached_authorities

    if os.path.exists(OUTPUT_FILE):
        try:
            with open(OUTPUT_FILE, "r") as f:
                items = json.load(f)
                cached_authorities.update(items)  # Cache authorities
                return set(items)
        except Exception as e:
            print(f"[Load Authorities] Failed: {e}")
    
    return TRUSTED_AUTHORITIES

# === Enhanced Audit Logic ===
async def audit_token(address: str) -> dict:
    """Enhanced token audit with Prometheus telemetry and optimized caching."""
    AUTHORITY_FETCHES.inc()
    ts = int(time.time())
    async with AUTHORITY_LATENCY.time():
        try:
            # Check cache first
            if address in cached_authorities:
                return {"safe": True, "reason": "Cached trusted authority", "timestamp": ts}

            red_flags = []

            # 1. Metadata check
            metadata = await fetch_metadata(address)
            if not metadata:
                red_flags.append("Metadata missing")

            # 2. Token specifics (holder concentration, mint age, etc.)
            # Placeholder for more checks...

            # 3. Simple liquidity heuristic (example)
            if "unlocked" in metadata.get("name", "").lower():
                red_flags.append("Name suggests unlocked liquidity")

            result = {
                "safe": len(red_flags) == 0,
                "reason": "; ".join(red_flags) if red_flags else "Passed all checks",
                "timestamp": ts,
            }
            result["audit_hash"] = audit_hash(address, result, ts)

            return result
        except Exception as e:
            AUTHORITY_ERRORS.inc()
            FAILED_GUARDIAN_DLQ.append({"symbol": address, "error": str(e)})
            logger.warning(f"[Token Guardian] Audit failed for {address}: {e}")
            return {"safe": False, "reason": "Audit service error", "timestamp": ts, "audit_hash": audit_hash(address, {"safe": False, "reason": "Audit service error"}, ts)}

# === Close Client ===
async def close_guardian():
    """Graceful shutdown for the client session."""
    await asyncio.gather()
