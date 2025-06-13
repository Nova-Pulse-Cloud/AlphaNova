import logging
from typing import Dict, Any, List
from solana.rpc.async_api import AsyncClient
from solana.publickey import PublicKey
import asyncio

logger = logging.getLogger("nova.orderbook_overlay")

class OrderBookOverlay:
    """
    Elite OrderBookOverlay - Monitors and analyzes the order book data (market depth)
    for on-chain tokens, detecting large price movements and market trends.
    """

    def __init__(self, rpc_url: str, symbol: str, depth_threshold: float = 100.0):
        """
        Initialize the OrderBookOverlay object.

        :param rpc_url: URL of the Solana or Ethereum RPC node.
        :param symbol: The trading symbol (e.g., 'SOL/USDT').
        :param depth_threshold: Threshold for detecting significant order book imbalances.
        """
        self.rpc_url = rpc_url
        self.symbol = symbol
        self.depth_threshold = depth_threshold
        self.client = AsyncClient(rpc_url)

    async def fetch_order_book(self) -> Dict[str, Any]:
        """
        Fetch the current order book for a given symbol (e.g., 'SOL/USDT').

        :return: A dictionary containing bid and ask orders.
        """
        try:
            # Fetch the order book from the RPC (for Solana or Ethereum)
            public_key = PublicKey(self.symbol)  # This can vary based on your symbol
            order_book_data = await self.client.get_orderbook(public_key)

            logger.info(f"Fetched order book for {self.symbol}")
            return order_book_data
        except Exception as e:
            logger.error(f"Error fetching order book for {self.symbol}: {e}")
            return {}

    async def analyze_market_depth(self, order_book_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analyze the market depth (order book) to detect large price imbalances.

        :param order_book_data: The order book data (bids/asks) to analyze.
        :return: A dictionary with market depth insights.
        """
        total_bid = sum(order["price"] * order["size"] for order in order_book_data["bids"])
        total_ask = sum(order["price"] * order["size"] for order in order_book_data["asks"])

        imbalance = total_bid - total_ask
        market_trend = "neutral"

        if imbalance > self.depth_threshold:
            market_trend = "bullish"
        elif imbalance < -self.depth_threshold:
            market_trend = "bearish"

        logger.info(f"Market Trend for {self.symbol}: {market_trend} | Bid-Ask Imbalance: {imbalance}")
        return {
            "symbol": self.symbol,
            "market_trend": market_trend,
            "imbalance": imbalance,
        }

    async def get_order_book_summary(self) -> Dict[str, Any]:
        """
        Get a summarized view of the current order book and market depth.

        :return: A summary of the order book (market trend, imbalance, etc.).
        """
        order_book_data = await self.fetch_order_book()
        if not order_book_data:
            return {}

        summary = await self.analyze_market_depth(order_book_data)
        return summary

    async def track_significant_price_movement(self, previous_price: float, current_price: float) -> bool:
        """
        Track significant price movement based on current and previous prices.

        :param previous_price: The price from the previous check.
        :param current_price: The current price from the order book.
        :return: Boolean indicating if a significant price movement occurred.
        """
        price_change = abs(current_price - previous_price) / previous_price
        significant_movement = price_change >= 0.05  # 5% price movement threshold for "significant"

        if significant_movement:
            logger.info(f"Significant price movement detected for {self.symbol}: {price_change * 100:.2f}%")
        return significant_movement

# Example Usage
async def run_example():
    rpc_url = "https://api.mainnet-beta.solana.com"
    symbol = "SOL/USDT"
    orderbook = OrderBookOverlay(rpc_url=rpc_url, symbol=symbol)

    # Get order book summary for the symbol
    order_book_summary = await orderbook.get_order_book_summary()
    print(order_book_summary)

    # Track significant price movements
    previous_price = 120.0
    current_price = 125.0
    is_significant = await orderbook.track_significant_price_movement(previous_price, current_price)
    print(f"Significant movement: {is_significant}")
