import logging
import base64
import httpx
from typing import Dict, Optional
from solana.keypair import Keypair
from solana.rpc.async_api import AsyncClient
from solana.transaction import Transaction
from solders.signature import Signature
from prometheus_client import Counter, Histogram
from utils.config import Config

logger = logging.getLogger("wallet_manager")

# Prometheus metrics
WALLET_TX_COUNT         = Counter("wallet_tx_count", "Total transactions signed by wallet")
WALLET_TX_FAILS         = Counter("wallet_tx_failures", "Wallet TX failures")
WALLET_TX_LATENCY       = Histogram("wallet_tx_latency_seconds", "Latency of wallet tx commits")
WALLET_TX_CONFIRMS      = Counter("wallet_tx_confirm_success", "Confirmed transactions")
WALLET_TX_CONFIRM_FAILS = Counter("wallet_tx_confirm_failures", "Failed confirmations")

class WalletEntry:
    """
    Metadata container for a single wallet.
    """
    def __init__(self, chain: str, keypair: Keypair, label: Optional[str] = None):
        self.chain   = chain
        self.keypair = keypair
        self.label   = label or f"{chain}-wallet"
        self.pubkey  = keypair.public_key

    def summary(self) -> Dict[str, str]:
        return {
            "chain": self.chain,
            "pubkey": str(self.pubkey),
            "label": self.label,
        }

class WalletManager:
    """
    Manages wallets for Nova Pulse, including signing and broadcasting transactions.
    """
    def __init__(self, config: Config):
        self.config   = config
        self.wallets: Dict[str, WalletEntry] = {}
        self.redis_url = config.REDIS_URL
        self._init_wallets()

    def _init_wallets(self):
        # Load Solana keypair from config
        secret_bytes = bytes(self.config.SOL_PRIVATE_KEY)
        sol_kp        = Keypair.from_secret_key(secret_bytes)
        self.add_wallet("solana", sol_kp, label="primary")

    def add_wallet(self, chain: str, keypair: Keypair, label: Optional[str] = None):
        self.wallets[chain] = WalletEntry(chain, keypair, label)
        logger.info(f"[WalletManager] Wallet added for {chain} ({label})")

    def get_wallet(self, chain: str) -> WalletEntry:
        if chain not in self.wallets:
            raise ValueError(f"[WalletManager] Wallet for chain '{chain}' not configured.")
        return self.wallets[chain]

    def get_pubkey(self, chain: str) -> str:
        return str(self.get_wallet(chain).pubkey)

    async def submit_swap(
        self,
        symbol: str,
        quote: dict,
        amount: float,
        user_id: Optional[str] = None
    ) -> tuple[str, float]:
        """
        Executes a token swap via Jupiter API and broadcasts the transaction.
        Returns (signature, received_amount).
        """
        wallet  = self.get_wallet("solana")
        rpc_url = self.config.HELIUS_RPC

        if not rpc_url:
            raise ValueError("Missing HELIUS_RPC in config")

        # Build swap transaction via Jupiter
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                "https://quote-api.jup.ag/v6/swap",
                json={
                    "userPublicKey": str(wallet.pubkey),
                    "wrapUnwrapSOL": True,
                    "quoteResponse": quote,
                    "computeUnitPriceMicroLamports": 10000,
                }
            )
            data = resp.json()
            tx_b64 = data.get("swapTransaction")
            if not tx_b64:
                raise Exception("Jupiter did not return a transaction.")

        # Decode, deserialize, sign
        tx_bytes = base64.b64decode(tx_b64)
        tx = Transaction.deserialize(tx_bytes)
        tx.sign([wallet.keypair])

        # Send to Solana
        async with WALLET_TX_LATENCY.time():
            client = AsyncClient(rpc_url)
            sig_resp = await client.send_raw_transaction(tx.serialize())
            confirm = await client.confirm_transaction(sig_resp.value)
            await client.close()

        sig_str = str(sig_resp.value) if isinstance(sig_resp, Signature) else sig_resp
        logger.info(f"[WalletManager] TX sent: {sig_str} → https://solscan.io/tx/{sig_str}")
        WALLET_TX_COUNT.inc()
        if confirm.value:
            WALLET_TX_CONFIRMS.inc()
        else:
            WALLET_TX_CONFIRM_FAILS.inc()

        return sig_str, quote.get("outAmount", 0) / 1e9

    # Placeholder methods to implement:
    async def get_total_open_sol(self) -> float:
        raise NotImplementedError

    async def has_open_position(self, symbol: str) -> bool:
        raise NotImplementedError
