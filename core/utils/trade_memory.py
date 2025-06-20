import csv
import hashlib
import io
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from prometheus_client import Counter, Gauge

from core.utils.config import config, Config

logger = logging.getLogger("nova.trade_memory")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TRADE_ADDED = Counter("trade_memory_add_total", "Trades recorded")
TRADE_PNL_TOTAL = Gauge("trade_memory_pnl_total", "Aggregate PnL per symbol", ["symbol"])
TRADE_WINRATE_GAUGE = Gauge("trade_memory_win_rate", "Win rate per symbol", ["symbol"])

# ────────────────────────────────────────────────────────────────────────────────
# SQLite Schema (single table – append-only)
# ────────────────────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS trades (
    id        TEXT PRIMARY KEY,
    symbol    TEXT NOT NULL,
    entry     REAL,
    exit      REAL,
    size      REAL,
    pnl       REAL,
    timestamp INTEGER
);
"""

class TradeMemory:
    """
    Thread-safe in-memory cache plus persistent SQLite ledger for
    real trades with win-rate and PnL analytics, fully instrumented.
    """
    def __init__(self, db_path: Optional[Union[str, Path, Config]] = None) -> None:
        # Resolve db_path if passed a Config instance
        if isinstance(db_path, Config):
            db_path = getattr(db_path, "TRADE_MEMORY_DB_PATH", "trade_memory.db")
        # Determine actual file path
        self._db_path = Path(db_path or getattr(config, "TRADE_MEMORY_DB_PATH", "trade_memory.db"))
        self._lock = threading.RLock()
        self._history: List[Dict[str, Any]] = []
        self._by_symbol: Dict[str, List[Dict[str, Any]]] = {}

        # Initialize SQLite connection
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        self._conn.execute(DDL)
        self._bootstrap_from_db()

    def _bootstrap_from_db(self) -> None:
        cur = self._conn.execute("SELECT id, symbol, entry, exit, size, pnl, timestamp FROM trades ORDER BY timestamp ASC")
        rows = cur.fetchall()
        for row in rows:
            rec = {
                "id": row[0],
                "symbol": row[1],
                "entry": float(row[2]),
                "exit": float(row[3]),
                "size": float(row[4]),
                "pnl": float(row[5]),
                "timestamp": int(row[6]),
            }
            self._ingest_record(rec, persist=False)
        logger.info(
            "[TradeMemory] Bootstrapped %d trades from %s", len(self._history), self._db_path
        )

    def _persist_trade(self, rec: Dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO trades (id, symbol, entry, exit, size, pnl, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                rec["id"], rec["symbol"], rec.get("entry"), rec.get("exit"),
                rec.get("size"), rec.get("pnl"), rec["timestamp"],
            ),
        )

    def add_trade(self, symbol: str, entry: float, size: float, exit: Optional[float] = None) -> None:
        """
        Record a new trade and update metrics.
        :param symbol: Trading symbol (str)
        :param entry: Entry price (float)
        :param size: Trade size in SOL (float)
        :param exit: Exit price (float, None if still open)
        """
        rec: Dict[str, Any] = {
            "symbol": symbol,
            "entry": float(entry),
            "exit": float(exit) if exit is not None else 0.0,
            "size": float(size),
            "pnl": (float(exit) - float(entry)) * float(size) if exit is not None else 0.0,
            "timestamp": int(time.time()),
        }
        rec["id"] = rec.get("id") or self._hash_trade(rec)

        with self._lock:
            self._ingest_record(rec, persist=True)

        TRADE_ADDED.inc()
        self._update_metrics(symbol)

    def get_history(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._history)

    def get_symbol_history(self, symbol: str) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._by_symbol.get(symbol, []))

    def win_rate(self, symbol: str) -> float:
        trades = self.get_symbol_history(symbol)
        if not trades:
            return 0.0
        wins = sum(1 for t in trades if t.get("pnl", 0) > 0)
        return wins / len(trades)

    def total_pnl(self, symbol: str) -> float:
        return sum(t.get("pnl", 0.0) for t in self.get_symbol_history(symbol))

    def latest_trade(self, symbol: str) -> Dict[str, Any]:
        trades = self.get_symbol_history(symbol)
        return trades[-1] if trades else {}

    # Alias for consistency
    def get_latest_trade(self, symbol: str) -> Dict[str, Any]:
        return self.latest_trade(symbol)

    def export_csv(self) -> str:
        headers = ["id", "symbol", "entry", "exit", "size", "pnl", "timestamp"]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=headers)
        writer.writeheader()
        with self._lock:
            for t in self._history:
                writer.writerow({h: t.get(h, "") for h in headers})
        return buf.getvalue()

    def _ingest_record(self, rec: Dict[str, Any], persist: bool) -> None:
        sym = rec["symbol"]
        self._history.append(rec)
        self._by_symbol.setdefault(sym, []).append(rec)
        if persist:
            self._persist_trade(rec)

    def _update_metrics(self, symbol: str) -> None:
        pnl = self.total_pnl(symbol)
        wr = self.win_rate(symbol)
        TRADE_PNL_TOTAL.labels(symbol=symbol).set(pnl)
        TRADE_WINRATE_GAUGE.labels(symbol=symbol).set(wr)

    @staticmethod
    def _hash_trade(trade: Dict[str, Any]) -> str:
        core = f"{trade.get('symbol')}:{trade.get('entry')}:{trade.get('exit')}:{trade.get('size')}:{trade.get('pnl')}:{trade.get('timestamp')}"
        return hashlib.sha256(core.encode()).hexdigest()

    def to_json(self, pretty: bool = False) -> str:
        with self._lock:
            return json.dumps(self._history, indent=2 if pretty else None)
