import logging
import threading
import csv
import io
import hashlib
import time
from typing import List, Dict, Any
from utils.config import Config

logger = logging.getLogger("nova.trade_memory")

class TradeMemory:
    """
    Elite Trade Memory - Stores and manages trade history, calculates win rates,
    and integrates with reinforcement learning for decision optimization.
    """
    def __init__(self, config: Config):
        """
        Initializes the TradeMemory object with configuration.
        :param config: Config instance providing environment settings.
        """
        self.config = config
        # Use configurable DB path or default
        self.db_path = getattr(config, "TRADE_MEMORY_DB_PATH", "trade_memory.db")

        # In-memory caches (backed by persistent store in future)
        self.trade_history: List[Dict[str, Any]] = []
        self.symbol_history: Dict[str, List[Dict[str, Any]]] = {}

        # Lock to ensure thread-safe updates
        self._lock = threading.Lock()

    def add_trade(self, symbol: str, trade_data: Dict[str, Any]) -> None:
        """
        Add a trade to the memory, updating both global and symbol-specific history.
        """
        # Enforce symbol key
        trade_data["symbol"] = symbol
        trade_data["timestamp"] = int(time.time())

        with self._lock:
            self.trade_history.append(trade_data)
            self.symbol_history.setdefault(symbol, []).append(trade_data)

        logger.info(f"Trade added for {symbol}: {trade_data}")

    def get_trade_history(self) -> List[Dict[str, Any]]:
        """
        Retrieve all trades from memory.
        """
        with self._lock:
            return list(self.trade_history)

    def get_symbol_history(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Retrieve trade history for a specific symbol.
        """
        with self._lock:
            return list(self.symbol_history.get(symbol, []))

    def get_win_rate(self, symbol: str) -> float:
        """
        Calculate and return the win rate for a given symbol.
        """
        trades = self.get_symbol_history(symbol)
        if not trades:
            return 0.0

        wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
        win_rate = wins / len(trades)
        logger.info(f"Win rate for {symbol}: {win_rate * 100:.2f}%")
        return win_rate

    def get_latest_trade(self, symbol: str) -> Dict[str, Any]:
        """
        Get the most recent trade for a specific symbol.
        """
        trades = self.get_symbol_history(symbol)
        return trades[-1] if trades else {}

    def calculate_pnl(self, symbol: str) -> float:
        """
        Calculate the total PnL (Profit and Loss) for a given symbol.
        """
        trades = self.get_symbol_history(symbol)
        total_pnl = sum(t.get("pnl", 0.0) for t in trades)
        logger.info(f"Total PnL for {symbol}: {total_pnl}")
        return total_pnl

    def get_trade_summary(self, symbol: str) -> Dict[str, Any]:
        """
        Get a summarized view of trade history for a symbol.
        """
        summary = {
            "symbol": symbol,
            "win_rate": self.get_win_rate(symbol),
            "total_pnl": self.calculate_pnl(symbol),
            "total_trades": len(self.get_symbol_history(symbol)),
        }
        logger.info(f"Trade summary for {symbol}: {summary}")
        return summary

    def generate_trade_hash(self, trade_data: Dict[str, Any]) -> str:
        """
        Generate a unique hash for a trade based on its core fields.
        """
        symbol = trade_data.get("symbol", "")
        entry  = trade_data.get("entry", 0)
        exit_p = trade_data.get("exit", 0)
        size   = trade_data.get("size", 0)
        pnl    = trade_data.get("pnl", 0)
        trade_str = f"{symbol}:{entry}:{exit_p}:{size}:{pnl}"
        return hashlib.sha256(trade_str.encode()).hexdigest()

    def export_trade_history(self) -> str:
        """
        Export the trade history to CSV format.
        """
        # Define headers for CSV
        headers = ["symbol", "entry", "exit", "size", "pnl", "timestamp"]

        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()

        with self._lock:
            if not self.trade_history:
                return output.getvalue()
            # Write each trade, filling missing fields with defaults
            for t in self.trade_history:
                row = {k: t.get(k, "") for k in headers}
                writer.writerow(row)

        return output.getvalue()
