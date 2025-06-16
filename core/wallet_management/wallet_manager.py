# core/wallet_management/wallet_manager.py

from __future__ import annotations

import asyncio
import base64
import logging
import json
from typing import Any, Dict, Tuple, Optional, List

import httpx
from prometheus_client import Counter, Histogram
from solders.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from solders.transaction import Transaction
from solders.signature import Signature  # type: ignore

from core.utils.config import config
from core.utils.pnl_tracker import add_fill
from core.utils.trade_memory import TradeMemory

logger = logging.getLogger("nova.wallet_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
WALLET_TX_COUNT        = Counter("wallet_tx_count",        "Total transactions signed by wallet")
WALLET_TX_FAILS        = Counter("wallet_tx_failures",     "Wallet TX failures")
WALLET_TX_LATENCY      = Histogram("wallet_tx_latency_seconds","Latency of wallet-TX commits")
WALLET_TX_CONFIRMS     = Counter("wallet_tx_confirm_success","Confirmed transactions")
WALLET_TX_CONFIRM_FAILS= Counter("wallet_tx_confirm_failures","Failed confirmations")

# ────────────────────────────────────────────────────────────────────────────────
# Keypair loader
# ────────────────────────────────────────────────────────────────────────────────
def _load_keypair() -> Keypair:
    """
    Loads a Solana Keypair from a 64-byte secret (list of ints) loaded from config or file.
    """
    # Patch: Accepts either direct list or file path (auto-detect)
    secret = config.SOL_PRIVATE_KEY
    if isinstance(secret, str):  # File path
        with open(secret, "r") as f:
            secret_list = json.load(f)
    else:  # Already loaded as list
        secret_list = secret
    secret_bytes = bytes(secret_list)
    return Keypair.from_secret_key(secret_bytes)

class WalletEntry:
    def __init__(self, chain: str, keypair: Keypair, label: Optional[str]=None):
        self.chain = chain
        self.keypair = keypair
        self.label = label or f"{chain}-wallet"
        self.pubkey = keypair.pubkey()  # PATCH: .pubkey() for solders

    def summary(self) -> Dict[str,str]:
        return {"chain": self.chain, "pubkey": str(self.pubkey), "label": self.label}

class WalletManager:
    """
    Async wallet & Jupiter swap manager for Solana.
    Single-user mode with PnL tracking and metrics.
    """
    def __init__(self) -> None:
        self.wallets: Dict[str, WalletEntry] = {}
        self._lock = asyncio.Lock()
        self.trade_memory = TradeMemory(config)  # ML memory
        self._init_wallet()

    def _init_wallet(self) -> None:
        kp = _load_keypair()
        self.wallets["solana"] = WalletEntry("solana", kp, label="primary")
        logger.info("[WalletManager] Solana wallet initialized: %s", kp.pubkey())  # PATCHED

    def get_wallet(self, chain: str="solana") -> WalletEntry:
        if chain not in self.wallets:
            raise ValueError(f"No wallet configured for chain '{chain}'")
        return self.wallets[chain]

    def get_pubkey(self, chain: str="solana") -> str:
        return str(self.get_wallet(chain).pubkey)

    async def submit_swap(
        self,
        quote: Dict[str,Any],
        *,
        user_id: Optional[str] = None,
    ) -> Tuple[str, float]:
        """
        Execute a Jupiter swap, record on-chain TX, and persist PnL.
        Returns (signature, output_amount_SOL).
        """
        wallet = self.get_wallet()
        # 1️⃣ Fetch signed transaction blob from Jupiter
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
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
                WALLET_TX_FAILS.inc()
                raise RuntimeError("Jupiter returned no transaction blob")
        # 2️⃣ Decode & sign
        tx = Transaction.deserialize(base64.b64decode(tx_b64))
        tx.sign([wallet.keypair])
        # 3️⃣ Broadcast
        async with self._lock, WALLET_TX_LATENCY.time():
            async with AsyncClient(config.HELIUS_RPC) as sol_client:
                send_resp = await sol_client.send_raw_transaction(tx.serialize())
                confirm = await sol_client.confirm_transaction(send_resp.value)
        sig = str(send_resp.value)
        WALLET_TX_COUNT.inc()
        if confirm.value:
            WALLET_TX_CONFIRMS.inc()
        else:
            WALLET_TX_CONFIRM_FAILS.inc()
        logger.info("[WalletManager] Swap TX %s → https://solscan.io/tx/%s", sig, sig)

        # 4️⃣ PnL tracking
        in_amount  = quote.get("inAmount", 0)  / 1e9
        out_amount = quote.get("outAmount",0) / 1e9
        pnl        = out_amount - in_amount
        side       = "BUY"  # or derive from signal context
        entry_price= in_amount  # mint amount treated as price
        exit_price = out_amount
        # offload DB write
        await asyncio.get_running_loop().run_in_executor(
            None,
            add_fill,
            wallet.chain,
            side,
            in_amount,
            entry_price,
            exit_price,
            pnl,
            user_id or "global",
            None,
        )

        return sig, out_amount

    async def get_total_open_sol(self) -> float:
        loop = asyncio.get_running_loop()
        history: List[Dict[str,Any]] = await loop.run_in_executor(
            None, self.trade_memory.get_history
        )
        return sum(t.get("size",0) for t in history if t.get("exit",0)==0)

    async def has_open_position(self, symbol: str) -> bool:
        loop = asyncio.get_running_loop()
        trades: List[Dict[str,Any]] = await loop.run_in_executor(
            None, self.trade_memory.get_symbol_history, symbol
        )
        return any(t.get("exit",0)==0 for t in trades)
