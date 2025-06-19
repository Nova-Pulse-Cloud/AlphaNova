# core/wallet_management/wallet_manager.py

from __future__ import annotations
import asyncio
import base64
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

import httpx
from solders.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from solders.transaction import Transaction
from prometheus_client import Counter, Histogram
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential, retry_if_exception_type

from core.utils.config import Config
from core.utils.trade_memory import TradeMemory
from core.utils.trade_logger import add_fill

logger = logging.getLogger("nova.wallet_manager")
logger.setLevel(logging.INFO)

# Metrics
WALLET_TX_COUNT = Counter(
    "wallet_tx_count", "Total transactions signed and sent by wallet"
)
WALLET_TX_FAILS = Counter(
    "wallet_tx_failures", "Number of failed wallet transactions"
)
WALLET_TX_LATENCY = Histogram(
    "wallet_tx_latency_seconds", "Latency of wallet transaction confirmations"
)
WALLET_TX_CONFIRMS = Counter(
    "wallet_tx_confirm_success", "Successful transaction confirmations"
)
WALLET_TX_CONFIRM_FAILS = Counter(
    "wallet_tx_confirm_failures", "Failed transaction confirmations"
)

class WalletEntry:
    def __init__(
        self,
        chain: str,
        keypair: Keypair,
        label: Optional[str] = None
    ) -> None:
        self.chain = chain
        self.keypair = keypair
        self.pubkey = keypair.pubkey()
        self.label = label or f"{chain}-wallet"

    def summary(self) -> Dict[str, str]:
        return {"chain": self.chain, "pubkey": str(self.pubkey), "label": self.label}

class WalletManager:
    """
    Manages Solana wallet interactions, swap execution, and PnL tracking.
    Provides robust retry logic and metrics instrumentation.
    """
    def __init__(self, config: Config) -> None:
        self.config = config
        self.wallets: Dict[str, WalletEntry] = {}
        self._lock = asyncio.Lock()
        self.trade_memory = TradeMemory(self.config)
        self._http: httpx.AsyncClient = httpx.AsyncClient(timeout=self.config.http_timeout)
        self._init_wallet()

    def _init_wallet(self) -> None:
        secret = self.config.SOL_PRIVATE_KEY
        if isinstance(secret, str):
            with open(secret, "r") as f:
                secret_list = json.load(f)
        else:
            secret_list = secret
        keypair = Keypair.from_secret_key(bytes(secret_list))
        self.wallets["solana"] = WalletEntry("solana", keypair, label="primary")
        logger.info("[WalletManager] Initialized Solana wallet: %s", keypair.pubkey())

    def get_wallet(self, chain: str = "solana") -> WalletEntry:
        if chain not in self.wallets:
            raise ValueError(f"No wallet configured for chain '{chain}'")
        return self.wallets[chain]

    def get_pubkey(self, chain: str = "solana") -> str:
        return str(self.get_wallet(chain).pubkey)

    async def submit_swap(
        self,
        quote: Dict[str, Any],
        *,
        user_id: Optional[str] = None
    ) -> Tuple[str, float]:
        """
        Executes a Jupiter swap: fetches transaction, signs, broadcasts,
        confirms on-chain, and tracks PnL.
        """
        wallet = self.get_wallet()
        swap_url = self.config.jupiter_swap_endpoint

        # Retry logic for network robustness
        async for attempt in AsyncRetrying(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type((httpx.HTTPError, asyncio.TimeoutError))
        ):
            with attempt:
                resp = await self._http.post(
                    swap_url,
                    json={
                        "userPublicKey": str(wallet.pubkey),
                        "wrapUnwrapSOL": True,
                        "quoteResponse": quote,
                        "computeUnitPriceMicroLamports": 10_000,
                    }
                )
                resp.raise_for_status()
                tx_b64 = resp.json().get("swapTransaction")
                if not tx_b64:
                    raise RuntimeError("Jupiter API did not return a swapTransaction blob")

        try:
            # Deserialize and sign transaction
            raw_tx = base64.b64decode(tx_b64)
            tx = Transaction.deserialize(raw_tx)
            tx.sign([wallet.keypair])

            # Broadcast with metrics
            with WALLET_TX_LATENCY.time():
                async with self._lock:
                    async with AsyncClient(self.config.HELIUS_RPC) as client:
                        send_resp = await client.send_raw_transaction(tx.serialize())
                        confirmation = await client.confirm_transaction(send_resp.value)

            signature = str(send_resp.value)
            WALLET_TX_COUNT.inc()
            if confirmation.value:
                WALLET_TX_CONFIRMS.inc()
            else:
                WALLET_TX_CONFIRM_FAILS.inc()
                logger.warning(
                    "[WalletManager] Confirmation failed for %s", signature
                )

            logger.info(
                "[WalletManager] Swap TX %s → https://solscan.io/tx/%s",
                signature,
                signature,
            )

            # Track PnL asynchronously
            in_amt = quote.get("inAmount", 0) / 1e9
            out_amt = quote.get("outAmount", 0) / 1e9
            pnl = out_amt - in_amt
            await asyncio.get_running_loop().run_in_executor(
                None,
                add_fill,
                wallet.chain,
                "BUY",
                in_amt,
                in_amt,
                out_amt,
                pnl,
                user_id or "global",
                None,
            )
            return signature, out_amt

        except Exception as err:
            WALLET_TX_FAILS.inc()
            logger.error("[WalletManager] submit_swap error: %s", err)
            raise

    async def get_total_open_sol(self) -> float:
        history = await asyncio.get_running_loop().run_in_executor(
            None, self.trade_memory.get_history
        )
        return sum(tx.get("size", 0) for tx in history if tx.get("exit", 0) == 0)

    async def has_open_position(self, symbol: str) -> bool:
        trades = await asyncio.get_running_loop().run_in_executor(
            None, self.trade_memory.get_symbol_history, symbol
        )
        return any(tx.get("exit", 0) == 0 for tx in trades)
