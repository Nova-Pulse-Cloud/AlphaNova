"""
Nova Pulse – TX Builder Elite (Jupiter/Phantom + Double Audit Failsafe)
"""
import asyncio
import hashlib
import logging
import time
from typing import Optional, Dict, Any
import base64

from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
from solana.rpc.types import TxOpts
from prometheus_client import Counter, Histogram
import httpx

from core.utils.risk_signals import get_cached_audit_verdict

log = logging.getLogger("tx_builder")

TX_BUILDS = Counter("tx_builds_total", "Total transactions built")
TX_FAILURES = Counter("tx_build_failures_total", "Failed transactions")
TX_LATENCY = Histogram("tx_build_latency_seconds", "Transaction build latency")
TX_ROUTE_CACHE_HITS = Counter("tx_route_cache_hits_total", "Cache hits for transaction routes")
TX_BLOCKED_AUDIT = Counter("tx_blocked_audit_total", "Transactions blocked by double-audit failsafe")

JUP_QUOTE_API = "https://quote-api.jup.ag/v6/quote"
JUP_SWAP_API = "https://quote-api.jup.ag/v6/swap"
route_cache: Dict[str, Dict[str, Any]] = {}

def provenance_hash(route_data: Dict[str, Any]) -> str:
    raw = str(route_data).encode()
    return hashlib.sha256(raw).hexdigest()

async def get_best_route(input_mint, output_mint, amount, slippage_bps):
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": amount,
        "slippageBps": slippage_bps,
        "swapMode": "ExactIn",
        "platformFeeBps": 0
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(JUP_QUOTE_API, params=params)
        r.raise_for_status()
        return r.json()["data"][0]

async def build_and_sign_tx(wallet: Keypair, route: dict, opts: TxOpts = None):
    body = {
        "route": route,
        "userPublicKey": str(wallet.pubkey()),
        "wrapUnwrapSOL": True,
        "feeAccount": None,
        "asLegacyTransaction": False
    }
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(JUP_SWAP_API, json=body)
        r.raise_for_status()
        swap_tx_base64 = r.json()["swapTransaction"]
    tx_bytes = base64.b64decode(swap_tx_base64)
    tx = VersionedTransaction.deserialize(tx_bytes)
    tx.sign([wallet])
    return tx

async def build_compressed_swap(
    wallet: Keypair,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 100,
    trace_id: Optional[str] = None,
    opts: Optional[TxOpts] = None,
    symbol: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    start = time.monotonic()
    trace_id = trace_id or "tx-" + hashlib.sha256(f"{input_mint}{output_mint}{amount}".encode()).hexdigest()[:8]
    try:
        if symbol:
            verdict = await get_cached_audit_verdict(symbol)
            if verdict:
                if not verdict.get("passed", False) or verdict.get("rug_score", 0.0) > 0.33:
                    log.warning(f"[TX:{trace_id}] Blocked by failsafe audit: {symbol} | Verdict: {verdict}")
                    TX_BLOCKED_AUDIT.inc()
                    return None
        route_key = f"{input_mint}->{output_mint}-{amount}-{slippage_bps}"
        route = route_cache.get(route_key)
        if not route:
            log.info(f"[TX:{trace_id}] Route not cached, fetching Jupiter route...")
            route = await get_best_route(input_mint, output_mint, amount, slippage_bps)
            if route:
                route_cache[route_key] = route
                TX_ROUTE_CACHE_HITS.inc()
            else:
                log.warning(f"[TX:{trace_id}] No route found for swap {input_mint}->{output_mint} amt:{amount}")
                TX_FAILURES.inc()
                return None
        tx: VersionedTransaction = await build_and_sign_tx(
            wallet=wallet,
            route=route,
            opts=opts or TxOpts(skip_preflight=True, max_retries=2)
        )
        TX_BUILDS.inc()
        TX_LATENCY.observe(time.monotonic() - start)
        tx_meta = {
            "tx": tx,
            "route": route,
            "hash": provenance_hash(route),
            "trace_id": trace_id
        }
        log.info(f"[TX:{trace_id}] TX built successfully, hash: {tx_meta['hash']}")
        return tx_meta
    except Exception as e:
        TX_FAILURES.inc()
        log.exception(f"[TX:{trace_id}] TX build failed: {e}")
        return None
