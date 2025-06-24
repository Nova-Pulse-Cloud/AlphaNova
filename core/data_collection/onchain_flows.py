# core/data_collection/onchain_flows.py
"""
Nova Pulse ALPHA – On-Chain Flows 2.1
-------------------------------------
• 100% async Solana flow scanner (live only – no mocks)
• Whale-tracker, liquidity drift, audit-hash, plugin events
• Modern Prometheus metrics and graceful connect/close
• Fully race-free, no deprecated APIs, session sharing
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from prometheus_client import Counter, Histogram
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient

from core.utils.config import Config, config
from core.utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.onchain_flows")
logger.setLevel(logging.INFO)

# Prometheus metrics
OCF_TX_FETCH  = Counter("ocf_tx_fetch_total",    "Transactions fetched")
OCF_ERRORS    = Counter("ocf_errors_total",      "On-chain-flow errors")
OCF_LATENCY   = Histogram("ocf_latency_seconds", "RPC latency")
OCF_WHALE_EVT = Counter("ocf_whale_events_total", "Whale-size transfers detected")

def _hash(obj: Any) -> str:
    return hashlib.sha256(str(obj).encode()).hexdigest()

class OnChainFlows:
    """Live transaction-flow analytics for Solana tokens/wallets."""
    def __init__(self, cfg: Optional[Config] = None, *, whale_thresh: float = 1_000):
        self.cfg = cfg or config
        self.whale_thresh = whale_thresh  # SOL units
        self._client: Optional[AsyncClient] = None
        self._sem = asyncio.Semaphore(4)

    async def connect(self):
        if self._client is None:
            self._client = AsyncClient(self.cfg.HELIUS_RPC)
            logger.info("[OnChainFlows] RPC client initialized → %s", self.cfg.HELIUS_RPC)

    async def close(self):
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("[OnChainFlows] RPC client closed.")

    async def fetch_signatures(self, address: str, limit: int = 100) -> List[str]:
        """Return transaction signatures for a given address."""
        await self.connect()
        addr = Pubkey.from_string(address)
        async with self._sem, OCF_LATENCY.time():
            resp = await self._client.get_signatures_for_address(addr, limit=limit)
        if resp.error:
            OCF_ERRORS.inc()
            raise RuntimeError(resp.error)
        OCF_TX_FETCH.inc(len(resp.value))
        return [sig.signature for sig in resp.value]

    async def get_tx_details(self, sig: str) -> Dict[str, Any]:
        """Fetch full transaction details by signature."""
        await self.connect()
        async with self._sem, OCF_LATENCY.time():
            resp = await self._client.get_transaction(sig, max_supported_transaction_version=0)
        if resp.error:
            OCF_ERRORS.inc()
            raise RuntimeError(resp.error)
        return resp.value

    async def fetch_transactions(self, wallet: str, *, limit: int = 100) -> List[Dict[str, Any]]:
        """Decode the last `limit` transactions for `wallet`."""
        sigs = await self.fetch_signatures(wallet, limit=limit)
        txs = await asyncio.gather(*(self.get_tx_details(s) for s in sigs))
        return [t for t in txs if t]

    async def token_flows(self, symbol: str, wallet: str, *, limit: int = 100) -> Dict[str, Any]:
        """Aggregate inflow/outflow & whale hits for `symbol` at `wallet`."""
        txs = await self.fetch_transactions(wallet, limit=limit)
        inflow = outflow = 0.0
        whales: List[Dict[str, Any]] = []

        for tx in txs:
            meta = tx.get("meta") or {}
            pre  = meta.get("preBalances", [0])[0] / 1e9
            post = meta.get("postBalances", [0])[0] / 1e9
            delta = post - pre
            if abs(delta) < 1e-9:
                continue
            if delta > 0:
                inflow += delta
            else:
                outflow += abs(delta)
            if abs(delta) >= self.whale_thresh:
                OCF_WHALE_EVT.inc()
                whales.append({
                    "sig":    tx.get("transaction", {}).get("signatures", ["?"])[0],
                    "amount": delta,
                })

        analysis = {
            "symbol":       symbol,
            "wallet":       wallet,
            "inflow":       inflow,
            "outflow":      outflow,
            "whale_events": whales,
            "timestamp":    int(datetime.utcnow().timestamp()),
            "audit_hash":   _hash((symbol, wallet, inflow, outflow, len(whales))),
        }
        await fire_plugin_event("onchain_flow", analysis)
        return analysis

    async def whale_activity(self, wallet: str, *, threshold: Optional[float] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Return only whale-size transactions for `wallet`."""
        thr = threshold or self.whale_thresh
        txs = await self.fetch_transactions(wallet, limit=limit)
        return [
            t for t in txs
            if abs((t.get("meta", {}).get("postBalances", [0])[0]
                    - t.get("meta", {}).get("preBalances", [0])[0]) / 1e9) >= thr
        ]

# Convenience helper
async def get_token_flow_stats(symbol: str, wallet: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    svc = OnChainFlows(config)
    return await svc.token_flows(symbol, wallet or config.SOL_ADDRESS, limit=limit)
