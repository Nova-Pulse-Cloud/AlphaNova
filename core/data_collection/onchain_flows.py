import logging
from typing import Dict, Any, List
from solana.rpc.async_api import AsyncClient
from solana.publickey import PublicKey
import asyncio

logger = logging.getLogger("nova.onchain_flows")

class OnChainFlows:
    """
    Elite OnChainFlows - Monitors and analyzes on-chain transaction flows and token movements.
    Provides insights into liquidity, whale transactions, and market trends.
    """

    def __init__(self, rpc_url: str, threshold: float = 1000.0):
        """
        Initialize the OnChainFlows object.
        
        :param rpc_url: URL to connect to the Solana or Ethereum RPC node.
        :param threshold: Threshold to detect significant transaction size (in native units, e.g., SOL or ETH).
        """
        self.rpc_url = rpc_url
        self.threshold = threshold
        self.client = AsyncClient(rpc_url)

    async def fetch_transactions(self, wallet_address: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch recent transactions for a wallet.
        
        :param wallet_address: The wallet address to check for recent transactions.
        :param limit: The number of transactions to fetch (default: 100).
        :return: A list of transaction details.
        """
        public_key = PublicKey(wallet_address)
        try:
            # Fetch the transaction history for the wallet
            result = await self.client.get_confirmed_signature_for_address2(public_key, limit=limit)
            transactions = result['result']
            logger.info(f"Fetched {len(transactions)} transactions for wallet {wallet_address}")
            return transactions
        except Exception as e:
            logger.error(f"Error fetching transactions for {wallet_address}: {e}")
            return []

    async def analyze_token_flows(self, symbol: str, transactions: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze the token flows (inflow/outflow) for a specific symbol.
        
        :param symbol: The token symbol (e.g., 'SOL').
        :param transactions: A list of transactions to analyze.
        :return: A dictionary with aggregated flow analysis.
        """
        inflows = 0.0
        outflows = 0.0
        significant_transactions = []

        for txn in transactions:
            amount = txn['token_amount']['uiAmount']
            if amount >= self.threshold:
                if txn['token_amount']['uiAmountIn'] > 0:
                    inflows += amount
                else:
                    outflows += amount
                significant_transactions.append(txn)

        logger.info(f"Token flow analysis for {symbol}: Inflows={inflows}, Outflows={outflows}")
        return {
            "symbol": symbol,
            "inflows": inflows,
            "outflows": outflows,
            "significant_transactions": significant_transactions,
        }

    async def monitor_whale_activity(self, wallet_address: str, threshold: float = 1000.0) -> List[Dict[str, Any]]:
        """
        Monitor whale activities: large transfers above a specific threshold.
        
        :param wallet_address: The wallet address to monitor for large transfers.
        :param threshold: Threshold for significant transfers (default: 1000 units).
        :return: A list of significant whale activities.
        """
        transactions = await self.fetch_transactions(wallet_address)
        significant_whales = []

        for txn in transactions:
            amount = txn['token_amount']['uiAmount']
            if amount >= threshold:
                significant_whales.append(txn)

        logger.info(f"Whale activity detected for {wallet_address}: {len(significant_whales)} large transactions.")
        return significant_whales

    async def track_onchain_flows(self, symbol: str, wallet_address: str) -> Dict[str, Any]:
        """
        Track and analyze on-chain flows for a given symbol and wallet.
        
        :param symbol: The token symbol to track.
        :param wallet_address: The wallet address to track for transactions.
        :return: A dictionary with token flow analysis.
        """
        transactions = await self.fetch_transactions(wallet_address)
        flow_analysis = await self.analyze_token_flows(symbol, transactions)
        return flow_analysis

    async def get_token_balance(self, wallet_address: str) -> float:
        """
        Get the token balance of a wallet.
        
        :param wallet_address: The wallet address to query.
        :return: The token balance.
        """
        try:
            public_key = PublicKey(wallet_address)
            balance = await self.client.get_balance(public_key)
            logger.info(f"Balance for wallet {wallet_address}: {balance['result']['value']} SOL")
            return balance['result']['value']
        except Exception as e:
            logger.error(f"Error fetching balance for wallet {wallet_address}: {e}")
            return 0.0

# Example Usage
async def run_example():
    rpc_url = "https://api.mainnet-beta.solana.com"
    onchain = OnChainFlows(rpc_url=rpc_url)

    # Track on-chain flows for a specific wallet (e.g., Solana wallet)
    symbol = "SOL"
    wallet_address = "YOUR_WALLET_ADDRESS"
    flow_data = await onchain.track_onchain_flows(symbol, wallet_address)
    print(flow_data)

    # Get token balance
    balance = await onchain.get_token_balance(wallet_address)
    print(f"Wallet Balance: {balance} {symbol}")
