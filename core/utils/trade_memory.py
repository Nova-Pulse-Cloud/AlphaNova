"""
Nova Pulse ALPHA – Elite Trade‑Memory Module
===========================================
Persistent, thread‑safe trade ledger with win‑rate / PnL analytics and
Prometheus instrumentation. Designed for *live* operation – no mocks, no
place‑holders.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from prometheus_client import Counter, Gauge

from utils.config import Config

logger = logging.getLogger("nova.trade_memory")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TRADE_ADDED         = Counter("trade_memory_add_total", "Trades recorded")
TRADE_PNL_TOTAL     = Gauge("trade_memory_pnl_total", "Aggregate PnL per symbol", ["symbol"])
TRADE_WINRATE_GAUGE = Gauge("trade_memory_win_rate", "Win‑rate per symbol", ["symbol"])

# ────────────────────────────────────────────────────────────────────────────────
# SQLite Schema (single table – append‑only)
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
    """Thread‑safe in‑memory cache **plus** persistent SQLite ledger."""

    def __init__(self, cfg: Optional[Config] = None):
        self._cfg: Config = cfg or Config()
        self._db_path: Path = Path(getattr(self._cfg, "TRADE_MEMORY_DB_PATH", "trade_memory.db"))

        # RLock because public getters may be called recursively by metrics
        self._lock = threading.RLock()

        # in‑memory mirrors (kept in sync with DB)
        self._history: List[Dict[str, Any]] = []
        self._by_symbol: Dict[str, List[Dict[str, Any]]] = {}

        # SQLite connection (allow cross‑thread, autocommit)
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False, isolation_level=None)
        self._conn.execute(DDL)
        self._bootstrap_from_db()

    # ────────────────────────────────────────────────────────────────────────
    # Bootstrap & persistence helpers
    # ────────────────────────────────────────────────────────────────────────
    def _bootstrap_from_db(self) -> None:
        cur = self._conn.execute("SELECT * FROM trades ORDER BY timestamp ASC")
        for row in cur.fetchall():
            rec = {
                "id": row[0],
                "symbol": row[1],
                "entry": row[2],
                "exit": row[3],
                "size": row[4],
                "pnl": row[5],
                "timestamp": row[6],
            }
            self._ingest_record(rec, persist=False)
        logger.info("[TradeMemory] Bootstrapped %d historical trades from %s", len(self._history), self._db_path)

    def _persist_trade(self, rec: Dict[str, Any]) -> None:
        self._conn.execute(
            "INSERT INTO trades (id, symbol, entry, exit, size, pnl, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                rec["id"],
                rec["symbol"],
                rec.get("entry"),
                rec.get("exit"),
                rec.get("size"),
                rec.get("pnl"),
                rec["timestamp"],
            ),
        )

    # ────────────────────────────────────────────────────────────────────────
    # Public API
    # ────────────────────────────────────────────────────────────────────────
    def add_trade(self, symbol: str, trade: Dict[str, Any]) -> None:
        """Append a *live* trade and update metrics."""
        trade = trade.copy()
        trade["symbol"] = symbol
        trade.setdefault("timestamp", int(time.time()))
        trade["id"] = trade.get("id") or self._hash_trade(trade)

        with self._lock:
            self._ingest_record(trade, persist=True)

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

    def export_csv(self) -> str:
        headers = ["symbol", "entry", "exit", "size", "pnl", "timestamp"]
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=headers)
        writer.writeheader()
        with self._lock:
            for t in self._history:
                writer.writerow({h: t.get(h, "") for h in headers})
        return buf.getvalue()

    # ────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ────────────────────────────────────────────────────────────────────────
    def _ingest_record(self, rec: Dict[str, Any], *, persist: bool) -> None:
        sym = rec["symbol"]
        self._history.append(rec)
        self._by_symbol.setdefault(sym, []).append(rec)
        if persist:
            self._persist_trade(rec)

    def _update_metrics(self, symbol: str) -> None:
        pnl = self.total_pnl(symbol)
        wr  = self.win_rate(symbol)
        TRADE_PNL_TOTAL.labels(symbol=symbol).set(pnl)
        TRADE_WINRATE_GAUGE.labels(symbol=symbol).set(wr)

    @staticmethod
    def _hash_trade(trade: Dict[str, Any]) -> str:
        core = f"{trade.get('symbol')}:{trade.get('entry')}:{trade.get('exit')}:{trade.get('size')}:{trade.get('pnl')}:{trade.get('timestamp')}"
        return hashlib.sha256(core.encode()).hexdigest()

    # ────────────────────────────────────────────────────────────────────────
    # Debug helper – JSON dump (non‑prod)
    # ────────────────────────────────────────────────────────────────────────
    def to_json(self, pretty: bool = False) -> str:
        with self._lock:
            return json.dumps(self._history, indent=2 if pretty else None)
