from typing import Optional, Dict, Any
from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
from solana.rpc.types import TxOpts
from core.jup_router import get_best_route, build_and_sign_tx
from prometheus_client import Counter, Histogram
import logging
import hashlib
import time
import asyncio

log = logging.getLogger("tx_builder")

# Prometheus metrics
TX_BUILDS = Counter("tx_builds_total", "Total transactions built")
TX_FAILURES = Counter("tx_build_failures_total", "Failed transactions")
TX_LATENCY = Histogram("tx_build_latency_seconds", "Transaction build latency")
TX_ROUTE_CACHE_HITS = Counter("tx_route_cache_hits_total", "Cache hits for transaction routes")

# In-memory route cache for performance optimization
route_cache: Dict[str, Dict[str, Any]] = {}

# === Provenance Hash (audit) ===
def provenance_hash(route_data: Dict[str, Any]) -> str:
    """Generates a provenance hash for the transaction route."""
    raw = str(route_data).encode()
    return hashlib.sha256(raw).hexdigest()

# === Build Compressed Jupiter Swap ===
async def build_compressed_swap(
    wallet: Keypair,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: int = 100,
    trace_id: Optional[str] = None,
    opts: Optional[TxOpts] = None
) -> Optional[Dict[str, Any]]:
    """Builds a compressed Jupiter swap transaction."""
    start = time.monotonic()
    trace_id = trace_id or "tx-" + hashlib.sha256(f"{input_mint}{output_mint}{amount}".encode()).hexdigest()[:8]
    
    try:
        # Check the cache for the route first
        route_key = f"{input_mint}->{output_mint}-{amount}-{slippage_bps}"
        route = route_cache.get(route_key)
        
        if not route:
            log.info(f"[TX:{trace_id}] Route not cached, fetching route...")
            route = await get_best_route(
                input_mint=input_mint,
                output_mint=output_mint,
                amount=amount,
                slippage_bps=slippage_bps
            )
            if route:
                route_cache[route_key] = route  # Cache the route for future use
                TX_ROUTE_CACHE_HITS.inc()
            else:
                log.warning(f"[TX:{trace_id}] No route found for swap {input_mint}->{output_mint} amt:{amount}")
                TX_FAILURES.inc()
                return None

        # Build and sign transaction
        tx: VersionedTransaction = await build_and_sign_tx(
            wallet=wallet,
            route=route,
            opts=opts or TxOpts(skip_preflight=True, max_retries=2)
        )

        # Metrics
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
