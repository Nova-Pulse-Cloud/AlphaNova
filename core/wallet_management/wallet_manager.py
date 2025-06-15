from __future__ import annotations

import asyncio
import base64
import json
import logging
from pathlib import Path
from typing import Any, Dict, Tuple, List

import httpx
from prometheus_client import Counter, Histogram
from solana.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from solana.transaction import Transaction
from solders.signature import Signature

from core.utils.config import config
from core.utils.trade_memory import TradeMemory

# ────────────────────────────────────────────────────────────────────────────────
# Logging & Metrics
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("nova.wallet_manager")
logger.setLevel(logging.INFO)

WALLET_TX_COUNT = Counter(
    "wallet_tx_count", "Total transactions signed by wallet"
)
WALLET_TX_FAILS = Counter(
    "wallet_tx_failures", "Wallet TX failures"
)
WALLET_TX_LATENCY = Histogram(
    "wallet_tx_latency_seconds", "Latency of wallet‑TX commits"
)
WALLET_TX_CONFIRMS = Counter(
    "wallet_tx_confirm_success", "Confirmed transactions"
)
WALLET_TX_CONFIRM_FAILS = Counter(
    "wallet_tx_confirm_failures", "Failed confirmations"
)

# ────────────────────────────────────────────────────────────────────────────────
# Helper – secure key loading
# ────────────────────────────────────────────────────────────────────────────────

def _load_keypair() -> Keypair:
    """Load a Solana Keypair from the mounted JSON secret file."""
    secret = config.SOL_PRIVATE_KEY  # list[int]
    return Keypair.from_secret_key(bytes(secret))


# ────────────────────────────────────────────────────────────────────────────────
# WalletEntry – represents one keypair on a chain
# ────────────────────────────────────────────────────────────────────────────────
class WalletEntry:
    def __init__(self, chain: str, keypair: Keypair, label: str | None = None):
        self.chain = chain
        self.keypair = keypair
        self.label = label or f"{chain}-wallet"
        self.pubkey = keypair.public_key

    def summary(self) -> Dict[str, str]:
        return {"chain": self.chain, "pubkey": str(self.pubkey), "label": self.label}


# ────────────────────────────────────────────────────────────────────────────────
# WalletManager – production-grade async wallet & swap handler
# ────────────────────────────────────────────────────────────────────────────────
class WalletManager:
    """
    Manages wallets, signs transactions, and executes swaps via Jupiter on Solana.
    Single-user (your Phantom burner wallet) mode. Fully async, concurrency-safe,
    with Prometheus metrics and persistent trade memory integration.
    """

    def __init__(self) -> None:
        self.cfg = config
        self.wallets: Dict[str, WalletEntry] = {}
        self._send_lock = asyncio.Lock()

        # Persistent trade history
        self.trade_memory = TradeMemory(self.cfg)

        self._init_wallets()

    # ── Initialization ─────────────────────────────────────────────────────────
    def _init_wallets(self) -> None:
        kp = _load_keypair()
        self.add_wallet("solana", kp, label="primary")
        logger.info(
            "[WalletManager] ✅ Solana wallet initialised → %s", kp.public_key
        )

    def add_wallet(
        self, chain: str, keypair: Keypair, *, label: str | None = None
    ) -> None:
        entry = WalletEntry(chain, keypair, label)
        self.wallets[chain] = entry
        logger.info(
            "[WalletManager] Wallet added for %s (pubkey=%s)", chain, entry.pubkey
        )

    # ── Accessors ──────────────────────────────────────────────────────────────
    def get_wallet(self, chain: str = "solana") -> WalletEntry:
        if chain not in self.wallets:
            raise ValueError(f"Wallet for chain '{chain}' not configured")
        return self.wallets[chain]

    def get_pubkey(self, chain: str = "solana") -> str:
        return str(self.get_wallet(chain).pubkey)

    # ── Swap Execution ─────────────────────────────────────────────────────────
    async def submit_swap(
        self,
        quote: Dict[str, Any],
        *,
        user_id: str | None = None,
    ) -> Tuple[str, float]:
        """
        Sign & send a Jupiter swap transaction.

        Args:
            quote: The JSON-encoded Jupiter quote response.
            user_id: Optional user identifier for logging.

        Returns:
            A tuple of (signature_str, output_amount_SOL).
        """
        wallet = self.get_wallet()

        # 1️⃣ Fetch swap transaction from Jupiter
        async with httpx.AsyncClient(timeout=15) as http_client:
            resp = await http_client.post(
                "https://quote-api.jup.ag/v6/swap",
                json={
                    "userPublicKey": str(wallet.pubkey),
                    "wrapUnwrapSOL": True,
                    "quoteResponse": quote,
                    "computeUnitPriceMicroLamports": 10_000,
                },
            )
            resp.raise_for_status()
            tx_b64 = resp.json().get("swapTransaction")
            if not tx_b64:
                raise RuntimeError("Jupiter API returned no transaction blob")

        # 2️⃣ Decode & sign transaction
        raw = base64.b64decode(tx_b64)
        tx = Transaction.deserialize(raw)
        tx.sign([wallet.keypair])

        # 3️⃣ Broadcast with lock & metrics
        async with self._send_lock, WALLET_TX_LATENCY.time():
            async with AsyncClient(self.cfg.HELIUS_RPC) as sol_client:
                send_resp = await sol_client.send_raw_transaction(
                    tx.serialize(), opts=None
                )
                confirm_resp = await sol_client.confirm_transaction(send_resp.value)

        sig = (
            str(send_resp.value)
            if isinstance(send_resp.value, (str, Signature))
            else str(send_resp)
        )
        logger.info(
            "[WalletManager] TX %s executed → https://solscan.io/tx/%s", sig, sig
        )

        # Update metrics
        WALLET_TX_COUNT.inc()
        if confirm_resp.value:
            WALLET_TX_CONFIRMS.inc()
        else:
            WALLET_TX_CONFIRM_FAILS.inc()

        # 4️⃣ Record in persistent trade memory (optional extension)
        # self.trade_memory.record_swap(...)

        # Convert lamports to SOL
        out_amount = quote.get("outAmount", 0) / 1e9
        return sig, out_amount

    # ── Portfolio Metrics ─────────────────────────────────────────────────────
    async def get_total_open_sol(self) -> float:
        """
        Sum of sizes for all open trades (exit price == 0).
        """
        loop = asyncio.get_running_loop()
        history: List[Dict[str, Any]] = await loop.run_in_executor(
            None, self.trade_memory.get_history
        )
        return sum(t.get("size", 0) for t in history if t.get("exit", 0) == 0)

    async def has_open_position(self, symbol: str) -> bool:
        """
        True if there is any open trade for the given symbol.
        """
        loop = asyncio.get_running_loop()
        trades: List[Dict[str, Any]] = await loop.run_in_executor(
            None, self.trade_memory.get_symbol_history, symbol
        )
        return any(t.get("exit", 0) == 0 for t in trades)
