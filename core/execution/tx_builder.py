# File: core/execution/tx_builder.py
# 🚀 Nova Pulse ALPHA – Elite TX Builder (Jupiter + Helius)
"""
Builds and signs compressed Jupiter swap transactions with route caching,
Prometheus metrics, and provenance hashing for full auditability.
"""

import asyncio
import hashlib
import logging
from time import monotonic
from typing import Any, Dict, Optional

import httpx
from prometheus_client import Counter, Histogram
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.types import TxOpts

from utils.config import Config
from utils.tpsl_jupiter_plugin import get_best_route, build_and_sign_tx

logger = logging.getLogger("nova.tx_builder")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TX_BUILDS             = Counter("tx_builds_total", "Total transactions built")
TX_FAILURES           = Counter("tx_build_failures_total", "Failed transactions")
TX_LATENCY            = Histogram("tx_build_latency_seconds", "Transaction build latency")
TX_ROUTE_CACHE_HITS   = Counter("tx_route_cache_hits_total", "Cache hits for transaction routes")
TX_ROUTE_CACHE_MISSES = Counter("tx_route_cache_misses_total", "Cache misses for routes")

# ────────────────────────────────────────────────────────────────────────────────
# In‑memory Route Cache (thread‑safe via asyncio.Lock)
# ────────────────────────────────────────────────────────────────────────────────
_route_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = asyncio.Lock()

# ────────────────────────────────────────────────────────────────────────────────
# Helper: Provenance Hash
# ────────────────────────────────────────────────────────────────────────────────
def _provenance_hash(route_data: Dict[str, Any]) -> str:
    """SHA‑256 hash for route provenance."""
    return hashlib.sha256(str(route_data).encode()).hexdigest()

# ────────────────────────────────────────────────────────────────────────────────
# Public API: build_compressed_swap
# ────────────────────────────────────────────────────────────────────────────────
async def build_compressed_swap(
    wallet: Keypair,
    input_mint: str,
    output_mint: str,
    amount: int,
    *,
    slippage_bps: Optional[int] = None,
    trace_id: Optional[str] = None,
    opts: Optional[TxOpts] = None,
    config: Optional[Config] = None,
) -> Optional[Dict[str, Any]]:
    """Return `{tx, route, hash, trace_id}` or **None** on failure."""

    cfg = config or Config()
    slippage = slippage_bps or cfg.JUP_SLIPPAGE_BPS
    trace_id = trace_id or f"tx-{hashlib.sha256(f'{input_mint}{output_mint}{amount}'.encode()).hexdigest()[:8]}"
    start = monotonic()

    route_key = f"{input_mint}->{output_mint}-{amount}-{slippage}"

    # ── Route Lookup (with async lock for concurrency) ──
    async with _cache_lock:
        route = _route_cache.get(route_key)

    if route:
        TX_ROUTE_CACHE_HITS.inc()
    else:
        TX_ROUTE_CACHE_MISSES.inc()
        logger.info(f"[TX:{trace_id}] Route cache miss → fetching {route_key}")
        try:
            route = await get_best_route(
                input_mint=input_mint,
                output_mint=output_mint,
                amount=amount,
                slippage_bps=slippage,
                rpc_url=cfg.HELIUS_RPC,
                timeout=cfg.JUP_FETCH_TIMEOUT,
            )
            if not route:
                TX_FAILURES.inc()
                logger.warning(f"[TX:{trace_id}] No route found for {route_key}")
                return None
            async with _cache_lock:
                _route_cache[route_key] = route
        except Exception as e:
            TX_FAILURES.inc()
            logger.exception(f"[TX:{trace_id}] Route fetch failed: {e}")
            return None

    # ── Build & Sign Transaction ──
    try:
        tx: VersionedTransaction = await build_and_sign_tx(
            wallet=wallet,
            route=route,
            opts=opts or TxOpts(skip_preflight=True, max_retries=cfg.SOL_MAX_RETRIES),
            rpc_url=cfg.HELIUS_RPC,
        )
        TX_BUILDS.inc()
        TX_LATENCY.observe(monotonic() - start)
        return {
            "tx": tx,
            "route": route,
            "hash": _provenance_hash(route),
            "trace_id": trace_id,
        }
    except Exception as e:
        TX_FAILURES.inc()
        logger.exception(f"[TX:{trace_id}] Transaction build failed: {e}")
        return None
