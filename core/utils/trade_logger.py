import os
import logging
import json
import hashlib
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Callable

import aiosqlite
from prometheus_client import Counter, Histogram

from core.utils.config import config

# ────────────────────────────────────────────────────────────────────────────────
# Logger & Metrics
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("nova.trade_logger")
logger.setLevel(logging.INFO)

TRADE_LOG_WRITES = Counter("trade_log_writes_total", "Trade log entries written")
TRADE_LOG_ERRORS = Counter("trade_log_errors_total", "Trade log errors")
TRADE_LOG_LATENCY = Histogram("trade_log_latency_seconds", "Latency of trade log writes")
TRADE_LOG_EXPORTS = Counter("trade_log_exports_total", "Trade logs exported")
TRADE_LOG_RETRY_ERRORS = Counter("trade_log_retry_errors_total", "Trade log retry errors")

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue & hooks
# ────────────────────────────────────────────────────────────────────────────────
FAILED_TRADE_LOG_DLQ: List[Dict] = []
trade_log_hooks: List[Callable] = []

def register_trade_log_hook(hook_fn: Callable):
    trade_log_hooks.append(hook_fn)

async def fire_trade_log_event(event_name: str, data: Dict):
    for hook in trade_log_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[TradeLogger] Hook error: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Database setup
# ────────────────────────────────────────────────────────────────────────────────
DB_PATH = Path(os.getenv("TRADE_LOG_DB_PATH", "trade_logger.db"))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)

async def setup_db() -> None:
    """Create trades table and indexes if not exist."""
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT,
                symbol TEXT,
                side TEXT,
                size REAL,
                entry REAL,
                exit REAL,
                pnl REAL,
                cycle TEXT,
                rationale TEXT,
                audit_hash TEXT,
                trace_id TEXT,
                rl_score REAL,
                sl_hit BOOLEAN,
                latency REAL,
                timestamp TEXT
            )
            """
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_trades_user_symbol ON trades(user, symbol)"
        )
        await db.commit()

# ────────────────────────────────────────────────────────────────────────────────
# Trade logging API
# ────────────────────────────────────────────────────────────────────────────────
async def log_trade(
    symbol: str,
    side: str,
    size: float,
    entry: float,
    exit: float,
    pnl: float,
    rationale: str,
    user: str = "global",
    cycle: Optional[str] = None,
    audit_hash: Optional[str] = None,
    trace_id: Optional[str] = None,
    rl_score: Optional[float] = None,
    sl_hit: Optional[bool] = None,
    latency: Optional[float] = None,
    timestamp: Optional[str] = None,
    **extra,
) -> None:
    """Insert a trade record into the SQLite DB and fire hooks."""
    TRADE_LOG_WRITES.inc()
    record = {
        "user": user,
        "symbol": symbol,
        "side": side,
        "size": size,
        "entry": entry,
        "exit": exit,
        "pnl": pnl,
        "cycle": cycle or "N/A",
        "rationale": rationale,
        "trace_id": trace_id or "N/A",
        "rl_score": rl_score,
        "sl_hit": sl_hit,
        "latency": latency,
        "timestamp": timestamp or datetime.utcnow().isoformat(),
    }
    if not audit_hash:
        audit_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

    try:
        async with TRADE_LOG_LATENCY.time():
            async with aiosqlite.connect(DB_PATH.as_posix()) as db:
                await db.execute(
                    """
                    INSERT INTO trades (
                        user, symbol, side, size, entry, exit, pnl,
                        cycle, rationale, audit_hash, trace_id,
                        rl_score, sl_hit, latency, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user,
                        symbol,
                        side,
                        size,
                        entry,
                        exit,
                        pnl,
                        cycle or "N/A",
                        rationale,
                        audit_hash,
                        trace_id,
                        rl_score,
                        sl_hit,
                        latency,
                        record["timestamp"],
                    ),
                )
                await db.commit()

        await fire_trade_log_event("trade_logged", {**record, "audit_hash": audit_hash})
    except Exception as e:
        TRADE_LOG_ERRORS.inc()
        FAILED_TRADE_LOG_DLQ.append({**record, "error": str(e)})
        await fire_trade_log_event("trade_log_error", {"error": str(e), "record": record})
        logger.error(f"[TradeLogger] Error logging trade: {e}")
        raise

async def get_trade_log(
    user: str = "global", symbol: Optional[str] = None, limit: int = 20
) -> str:
    """Retrieve latest trades as formatted text."""
    try:
        base_query = "SELECT * FROM trades WHERE user = ?"
        params: List = [user]
        if symbol:
            base_query += " AND symbol = ?"
            params.append(symbol)
        base_query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        async with aiosqlite.connect(DB_PATH.as_posix()) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(base_query, params)
            rows = await cursor.fetchall()

        if not rows:
            return "No trade logs available yet."

        lines = []
        for row in rows:
            ts = row["timestamp"][:19]
            line = (
                f"{ts} | {row['side'].upper()} {row['symbol']} "
                f"Size: {row['size']} | PnL: {row['pnl']:.4f} | Trace: {row['trace_id']}"
            )
            lines.append(line)
        return "\n".join(lines)
    except Exception as e:
        logger.error(f"[TradeLogger] Error fetching logs: {e}")
        return f"Error fetching logs: {e}"

async def export_trade_log(user: str = "global") -> List[Dict]:
    """Export full trade log as list of dicts."""
    TRADE_LOG_EXPORTS.inc()
    async with aiosqlite.connect(DB_PATH.as_posix()) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM trades WHERE user = ? ORDER BY timestamp", (user,))
        rows = await cursor.fetchall()
    return [dict(row) for row in rows]

async def replay_dlq() -> None:
    """Retry entries in the dead-letter queue."""
    for entry in FAILED_TRADE_LOG_DLQ.copy():
        try:
            await log_trade(**entry)
            FAILED_TRADE_LOG_DLQ.remove(entry)
        except Exception as e:
            TRADE_LOG_RETRY_ERRORS.inc()
            logger.error(f"[TradeLogger] Failed to replay DLQ entry: {e}")

def get_dlq() -> List[Dict]:
    """Return a copy of the current DLQ."""
    return FAILED_TRADE_LOG_DLQ.copy()
