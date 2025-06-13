import os
import json
import hashlib
import asyncio
import aiosqlite
from datetime import datetime
from typing import List, Dict, Optional, Callable
from prometheus_client import Counter, Histogram
import time

# === Prometheus metrics ===
TRADE_LOG_WRITES = Counter("trade_log_writes_total", "Trade log entries written")
TRADE_LOG_ERRORS = Counter("trade_log_errors_total", "Trade log errors")
TRADE_LOG_LATENCY = Histogram("trade_log_latency_seconds", "Latency of trade log writes")
TRADE_LOG_EXPORTS = Counter("trade_log_exports_total", "Trade logs exported")
TRADE_LOG_RETRY_ERRORS = Counter("trade_log_retry_errors_total", "Trade log retry errors")

# === DLQ and event hooks ===
FAILED_TRADE_LOG_DLQ: List[Dict] = []
trade_log_hooks: List[Callable] = []

def register_trade_log_hook(hook_fn: Callable):
    trade_log_hooks.append(hook_fn)

async def fire_trade_log_event(event_name: str, data: dict):
    for hook in trade_log_hooks:
        await hook(event_name, data)

DB_PATH = os.getenv("TRADE_LOG_DB_PATH", "trade_logger.db")

async def setup_db():
    """Setup the database and ensure the tables and indexes are created."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
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
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )""")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_trades_user_symbol ON trades(user, symbol)")
        await db.commit()

async def log_trade(symbol: str, side: str, size: float, entry: float, exit: float, pnl: float,
                    rationale: str, user: str = "global", cycle: Optional[str] = None,
                    audit_hash: Optional[str] = None, trace_id: Optional[str] = None,
                    rl_score: Optional[float] = None, sl_hit: Optional[bool] = None, latency: Optional[float] = None, timestamp: Optional[str] = None, **extra):
    """Logs a trade into the database with enhanced error handling and Prometheus metrics."""
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
        "timestamp": timestamp or datetime.utcnow().isoformat()
    }

    if not audit_hash:
        audit_hash = hashlib.sha256(json.dumps(record, sort_keys=True).encode()).hexdigest()

    try:
        async with TRADE_LOG_LATENCY.time():
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(""" 
                    INSERT INTO trades (user, symbol, side, size, entry, exit, pnl, cycle, rationale, audit_hash, trace_id, rl_score, sl_hit, latency, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (user, symbol, side, size, entry, exit, pnl, cycle or "N/A", rationale, audit_hash, trace_id, rl_score, sl_hit, latency, record["timestamp"]))
                await db.commit()

        enriched_record = {**record, "audit_hash": audit_hash}
        await fire_trade_log_event("trade_logged", enriched_record)
    except Exception as e:
        TRADE_LOG_ERRORS.inc()
        failed_entry = {**record, "error": str(e)}
        FAILED_TRADE_LOG_DLQ.append(failed_entry)
        await fire_trade_log_event("trade_log_error", {"error": str(e), "record": record})
        logger.error(f"[Trade Logger] Error logging trade: {e}")
        raise

async def get_trade_log(user: str = "global", symbol: Optional[str] = None, limit: int = 20) -> str:
    """Fetch the trade log for a specific user or symbol."""
    try:
        query = "SELECT * FROM trades WHERE user = ? ORDER BY timestamp DESC LIMIT ?"
        params = [user, limit]
        if symbol:
            query = "SELECT * FROM trades WHERE user = ? AND symbol = ? ORDER BY timestamp DESC LIMIT ?"
            params = [user, symbol, limit]
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()
        if not rows:
            return "No trade logs available yet."
        return "\n".join([f"{row['timestamp'][:19]} | {row['side'].upper()} {row['symbol']} | Size: {row['size']} | PnL: {row['pnl']:.2f} | Trace: {row['trace_id']} | Rationale: {row['rationale'][:60]}{'...' if len(row['rationale']) > 60 else ''}" for row in rows])
    except Exception as e:
        return f"Error fetching logs: {str(e)}"

async def export_trade_log(user: str = "global") -> List[Dict]:
    """Export the entire trade log for a specific user."""
    TRADE_LOG_EXPORTS.inc()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("SELECT * FROM trades WHERE user = ? ORDER BY timestamp", (user,))
        rows = await cursor.fetchall()
    return [dict(row) for row in rows]

async def replay_dlq():
    """Retry failed trade log entries from the DLQ."""
    for failed in FAILED_TRADE_LOG_DLQ[:]:
        try:
            await log_trade(**failed)
            FAILED_TRADE_LOG_DLQ.remove(failed)
        except Exception as e:
            logger.error(f"Failed to replay DLQ item: {e}")

def get_dlq():
    """Get the current DLQ."""
    return FAILED_TRADE_LOG_DLQ.copy()
