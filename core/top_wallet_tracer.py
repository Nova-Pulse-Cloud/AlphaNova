# File: core/top_wallet_tracer.py

import aiohttp
import logging

logger = logging.getLogger("whale_tracer")

WHALE_ADDRESSES = [
    # Replace with actual known whale wallets or load dynamically
    "SomeWhaleAddress1",
    "SomeWhaleAddress2"
]

EXPLORER_API_URL = "https://api.solscan.io/account/tokens?account={address}"

async def fetch_whale_tokens(address: str) -> list:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(EXPLORER_API_URL.format(address=address)) as resp:
                data = await resp.json()
                tokens = [t.get("tokenSymbol") for t in data.get("data", []) if t.get("tokenSymbol")]
                return tokens
        except Exception as e:
            logger.error(f"Failed fetching tokens for {address}: {e}")
            return []

async def get_recent_whale_tokens() -> str:
    output = []
    for addr in WHALE_ADDRESSES:
        tokens = await fetch_whale_tokens(addr)
        if tokens:
            output.append(f"{addr[-6:]}: {', '.join(tokens)}")
    return "\n".join(output) if output else "No recent whale activity detected."
