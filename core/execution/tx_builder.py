# File: core/execution/tx_builder.py

import logging
import hashlib
from time import monotonic
from typing import Optional, Dict, Any

import httpx
from prometheus_client import Counter, Histogram
from solders.transaction import VersionedTransaction
from solders.keypair import Keypair
from solana.rpc.types import TxOpts

from utils.config import Config
from utils.tpsl_jupiter_plugin import get_best_route, build_and_sign_tx  # updated import path

logger = logging.getLogger("tx_builder")
logger.setLevel(logging.INFO)

# Prometheus metrics
TX_BUILDS             = Counter("tx_builds_total", "Total transactions built")
TX_FAILURES           = Counter("tx_build_failures_total", "Failed transactions")
TX_LATENCY            = Histogram("tx_build_latency_seconds", "Transaction build latency")
TX_ROUTE_CACHE_HITS   = Counter("tx_route_cache_hits_total", "Cache hits for transaction routes")
TX_ROUTE_CACHE_MISSES = Counter("tx_route_cache_misses_total", "Cache misses for routes")

# In-memory route cache with a lock for concurrency safety
_route_cache: Dict[str, Dict[str, Any]] = {}
_cache_lock = httpx.AsyncClient()  # placeholder for a real lock (e.g. asyncio.Lock)

def _provenance_hash(route_data: Dict[str, Any]) -> str:
    raw = str(route_data).encode()
    return hashlib.sha256(raw).hexdigest()

async def build_compressed_swap(
    wallet: Keypair,
    input_mint: str,
    output_mint: str,
    amount: int,
    slippage_bps: Optional[int] = None,
    trace_id: Optional[str] = None,
    opts: Optional[TxOpts] = None,
    config: Optional[Config] = None
) -> Optional[Dict[str, Any]]:
    """
    Builds a compressed Jupiter swap transaction.
    Returns a dict with keys: tx, route, hash, trace_id; or None on failure.
    """
    cfg = config or Config()
    slippage = slippage_bps or cfg.JUP_SLIPPAGE_BPS
    start_ts = monotonic()
    trace_id = trace_id or f"tx-{hashlib.sha256(f'{input_mint}{output_mint}{amount}'.encode()).hexdigest()[:8]}"

    route_key = f"{input_mint}->{output_mint}-{amount}-{slippage}"
    # Route lookup
    if route := _route_cache.get(route_key):
        TX_ROUTE_CACHE_HITS.inc()
    else:
        TX_ROUTE_CACHE_MISSES.inc()
        logger.info(f"[TX:{trace_id}] Cache miss, fetching route {route_key}")
        try:
            route = await get_best_route(
                input_mint=input_mint,
                output_mint=output_mint,
                amount=amount,
                slippage_bps=slippage,
                rpc_url=cfg.HELIUS_RPC,
                timeout=cfg.JUP_FETCH_TIMEOUT
            )
            if not route:
                logger.warning(f"[TX:{trace_id}] No route for {route_key}")
                TX_FAILURES.inc()
                return None
            _route_cache[route_key] = route
        except Exception as e:
            TX_FAILURES.inc()
            logger.exception(f"[TX:{trace_id}] Route fetch failed: {e}")
            return None

    # Build & sign
    try:
        tx: VersionedTransaction = await build_and_sign_tx(
            wallet=wallet,
            route=route,
            opts=opts or TxOpts(skip_preflight=True, max_retries=cfg.SOL_MAX_RETRIES),
            rpc_url=cfg.HELIUS_RPC
        )
        TX_BUILDS.inc()
        TX_LATENCY.observe(monotonic() - start_ts)

        tx_meta = {
            "tx": tx,
            "route": route,
            "hash": _provenance_hash(route),
            "trace_id": trace_id
        }
        logger.info(f"[TX:{trace_id}] Built with hash {tx_meta['hash']}")
        return tx_meta

    except Exception as e:
        TX_FAILURES.inc()
        logger.exception(f"[TX:{trace_id}] Build failed: {e}")
        return None
