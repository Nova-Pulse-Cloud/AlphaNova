import os
import logging
import base64
import httpx
from typing import Dict, Optional, Union
from solana.keypair import Keypair
from solders.pubkey import Pubkey
from solana.rpc.async_api import AsyncClient
from solana.transaction import Transaction
from solders.signature import Signature
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram
import asyncio
import time

logger = logging.getLogger("wallet_manager")
load_dotenv()

# Prometheus metrics
WALLET_TX_COUNT = Counter("wallet_tx_count", "Total transactions signed by wallet")
WALLET_TX_FAILS = Counter("wallet_tx_failures", "Wallet TX failures")
WALLET_TX_LATENCY = Histogram("wallet_tx_latency_seconds", "Latency of wallet tx commits")
WALLET_TX_CONFIRMS = Counter("wallet_tx_confirm_success", "Confirmed transactions")
WALLET_TX_CONFIRM_FAILS = Counter("wallet_tx_confirm_failures", "Failed confirmations")

# ---- Secure Wallet Loading ----
def load_wallet_from_env(env_key: str) -> Keypair:
    pk = os.getenv(env_key)
    if not pk:
        raise ValueError(f"[WalletManager] Wallet env var '{env_key}' not found.")
    try:
        secret = [int(x) for x in pk.strip().split(",")]
        keypair = Keypair.from_secret_key(bytes(secret))
        return keypair
    except Exception as e:
        logger.error(f"[WalletManager] Failed to load wallet from {env_key}: {e}")
        raise

# ---- Wallet Metadata Model ----
class WalletEntry:
    def __init__(self, chain: str, keypair: Keypair, label: Optional[str] = None):
        self.chain = chain
        self.keypair = keypair
        self.label = label or f"{chain}-wallet"
        self.pubkey = keypair.public_key

    def summary(self) -> Dict[str, str]:
        return {
            "chain": self.chain,
            "pubkey": str(self.pubkey),
            "label": self.label,
        }

# ---- Multi-Wallet Manager ----
class WalletManager:
    """
    Manages all wallets for Nova Pulse.
    Supports dynamic wallet expansion and multi-chain support.
    """
    def __init__(self):
        self.wallets: Dict[str, WalletEntry] = {}
        self.redis_url = os.getenv("REDIS_URL", "")
        self._init_wallets()

    def _init_wallets(self):
        # Load wallets from environment
        sol_kp = load_wallet_from_env("SOL_PRIVATE_KEY")
        self.add_wallet("solana", sol_kp, label="primary")
        # Add support for future chains like SUI, EVM in the future

    def add_wallet(self, chain: str, keypair: Keypair, label: Optional[str] = None):
        self.wallets[chain] = WalletEntry(chain, keypair, label)
        logger.info(f"[WalletManager] Wallet added for {chain} ({label})")

    def get_wallet(self, chain: str) -> WalletEntry:
        if chain not in self.wallets:
            raise ValueError(f"[WalletManager] Wallet for chain '{chain}' not configured.")
        return self.wallets[chain]

    def get_pubkey(self, chain: str) -> str:
        return str(self.get_wallet(chain).pubkey)

    def list_wallets(self) -> Dict[str, Dict[str, str]]:
        return {k: w.summary() for k, w in self.wallets.items()}

    async def get_total_open_sol(self) -> float:
        # Placeholder for actual logic. Integrate with trade memory or PnL tracker.
        return 0.0

    async def has_open_position(self, symbol: str) -> bool:
        # Placeholder logic for open position checks
        return False

    async def submit_swap(self, symbol: str, quote: dict, amount: float, user_id: Optional[str] = None) -> tuple[str, float]:
        wallet = self.get_wallet("solana")
        user_pubkey = wallet.pubkey
        rpc_url = os.getenv("HELIUS_RPC")
        rpc_fallback = os.getenv("BACKUP_RPC")

        if not rpc_url:
            raise ValueError("Missing HELIUS_RPC env var for swap execution")

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(
                    "https://quote-api.jup.ag/v6/swap",
                    json={
                        "userPublicKey": str(user_pubkey),
                        "wrapUnwrapSOL": True,
                        "quoteResponse": quote,
                        "computeUnitPriceMicroLamports": 10000,
                    }
                )
                tx_data = resp.json()
                tx_b64 = tx_data.get("swapTransaction")
                if not tx_b64:
                    raise Exception("Jupiter did not return a transaction.")

            tx_bytes = base64.b64decode(tx_b64)
            tx = Transaction.deserialize(tx_bytes)
            tx.sign([wallet.keypair])

            client = AsyncClient(rpc_url)
            async with WALLET_TX_LATENCY.time():
                sig = await client.send_raw_transaction(tx.serialize())
                await client.confirm_transaction(sig.value)
            await client.close()

            sig_str = str(sig.value) if isinstance(sig, Signature) else sig
            logger.info(f"[WalletManager] TX sent: {sig_str} → https://solscan.io/tx/{sig_str}")
            WALLET_TX_COUNT.inc()
            WALLET_TX_CONFIRMS.inc()
            return sig_str, quote.get("outAmount", 0) / 1e9

        except Exception as e:
            WALLET_TX_FAILS.inc()
            WALLET_TX_CONFIRM_FAILS.inc()
            logger.error(f"[WalletManager] TX execution failed: {e}")

            # Retry logic with exponential backoff
            if rpc_fallback:
                try:
                    client = AsyncClient(rpc_fallback)
                    sig = await client.send_raw_transaction(tx.serialize())
                    await client.confirm_transaction(sig.value)
                    await client.close()

                    sig_str = str(sig.value)
                    logger.warning(f"[WalletManager] TX fallback success: {sig_str} → https://solscan.io/tx/{sig_str}")
                    WALLET_TX_COUNT.inc()
                    WALLET_TX_CONFIRMS.inc()
                    return sig_str, quote.get("outAmount", 0) / 1e9
                except Exception as e2:
                    logger.error(f"[WalletManager] TX fallback failed: {e2}")
            raise
