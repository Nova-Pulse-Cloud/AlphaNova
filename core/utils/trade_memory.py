import logging
from typing import List, Dict, Any
import hashlib
import time

logger = logging.getLogger("nova.trade_memory")

class TradeMemory:
    """
    Elite Trade Memory - Stores and manages trade history, calculates win rates,
    and integrates with reinforcement learning for decision optimization.
    """
    def __init__(self, db_path: str = "trade_memory.db"):
        """
        Initializes the TradeMemory object.
        
        :param db_path: Path to the database or file for storing trade history.
        """
        self.db_path = db_path
        self.trade_history: List[Dict[str, Any]] = []  # Stores trade history in memory
        self.symbol_history: Dict[str, List[Dict[str, Any]]] = {}  # Symbol-specific history for faster retrieval

    def add_trade(self, symbol: str, trade_data: Dict[str, Any]) -> None:
        """
        Add a trade to the memory and update symbol-specific history.
        
        :param symbol: The symbol for the trade (e.g., "BTC/USD").
        :param trade_data: The data for the trade (entry, exit, size, pnl, etc.).
        """
        trade_data["timestamp"] = int(time.time())
        self.trade_history.append(trade_data)

        if symbol not in self.symbol_history:
            self.symbol_history[symbol] = []
        self.symbol_history[symbol].append(trade_data)
        
        logger.info(f"Trade added for {symbol}: {trade_data}")

    def get_trade_history(self) -> List[Dict[str, Any]]:
        """
        Retrieve all trades from memory.
        
        :return: List of all trade history.
        """
        return self.trade_history

    def get_symbol_history(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Retrieve trade history for a specific symbol.
        
        :param symbol: The symbol to retrieve trade history for.
        :return: List of trades for the given symbol.
        """
        return self.symbol_history.get(symbol, [])

    def get_win_rate(self, symbol: str) -> float:
        """
        Calculate and return the win rate for a given symbol.
        
        :param symbol: The symbol to calculate the win rate for.
        :return: Win rate as a float (0.0 to 1.0).
        """
        symbol_trades = self.get_symbol_history(symbol)
        if not symbol_trades:
            return 0.0

        wins = sum(1 for trade in symbol_trades if trade["pnl"] > 0)
        total_trades = len(symbol_trades)

        win_rate = wins / total_trades
        logger.info(f"Win rate for {symbol}: {win_rate * 100:.2f}%")
        return win_rate

    def get_latest_trade(self, symbol: str) -> Dict[str, Any]:
        """
        Get the most recent trade for a specific symbol.
        
        :param symbol: The symbol to retrieve the latest trade for.
        :return: The most recent trade data for the symbol.
        """
        symbol_trades = self.get_symbol_history(symbol)
        if symbol_trades:
            return symbol_trades[-1]
        return {}

    def calculate_pnl(self, symbol: str) -> float:
        """
        Calculate the total PnL (Profit and Loss) for a given symbol.
        
        :param symbol: The symbol to calculate the PnL for.
        :return: The total PnL for the symbol.
        """
        symbol_trades = self.get_symbol_history(symbol)
        total_pnl = sum(trade.get("pnl", 0.0) for trade in symbol_trades)
        logger.info(f"Total PnL for {symbol}: {total_pnl}")
        return total_pnl

    def get_trade_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get a summarized view of trade history for a symbol.
        
        :param symbol: The symbol to get the trade summary for.
        :return: A dictionary with win rate, total PnL, and total trades.
        """
        win_rate = self.get_win_rate(symbol)
        total_pnl = self.calculate_pnl(symbol)
        total_trades = len(self.get_symbol_history(symbol))

        summary = {
            "symbol": symbol,
            "win_rate": win_rate,
            "total_pnl": total_pnl,
            "total_trades": total_trades,
        }
        logger.info(f"Trade summary for {symbol}: {summary}")
        return summary

    def generate_trade_hash(self, trade_data: Dict[str, Any]) -> str:
        """
        Generate a unique hash for a trade based on its data.
        
        :param trade_data: The trade data to generate the hash for.
        :return: A SHA256 hash string for the trade.
        """
        trade_str = f"{trade_data['symbol']}:{trade_data['entry']}:{trade_data['exit']}:{trade_data['size']}:{trade_data['pnl']}"
        return hashlib.sha256(trade_str.encode()).hexdigest()

    def export_trade_history(self) -> str:
        """
        Export the trade history to a CSV or another format.
        
        :return: Exported trade history as a string (for CSV, JSON, etc.).
        """
        import csv
        import io
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=self.trade_history[0].keys())
        writer.writeheader()
        writer.writerows(self.trade_history)
        return output.getvalue()
