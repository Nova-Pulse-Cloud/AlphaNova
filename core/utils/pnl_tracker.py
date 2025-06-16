# core/utils/pnl_tracker.py

import os
import sqlite3
import logging
import io
import matplotlib.pyplot as plt
from datetime import datetime
from typing import List, Dict, Optional, Any  # <-- Added Any

from prometheus_client import Counter, Histogram

from core.utils.config import config


logger = logging.getLogger("nova.pnl_tracker")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ────────────────────────────────────────────────────────────────────────────────
PNL_FILL_COUNT = Counter("pnl_fills_total", "Total PnL fill records inserted")
PNL_FILL_ERRORS = Counter("pnl_fill_errors_total", "Errors inserting PnL fill record")
PNL_QUERY_LATENCY = Histogram("pnl_query_latency_seconds", "Latency of PnL history queries")
PNL_IMAGE_LATENCY = Histogram("pnl_image_latency_seconds", "Latency of PnL curve image generation")
PNL_IMAGE_ERRORS = Counter("pnl_image_errors_total", "Errors generating PnL curve image")

# ────────────────────────────────────────────────────────────────────────────────
# Database setup
# ────────────────────────────────────────────────────────────────────────────────
DB_PATH = getattr(config, "PNL_DB_PATH", "pnl_tracker.db")

def _get_conn() -> sqlite3.Connection:
    """Return a SQLite connection to the PnL database."""
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = sqlite3.connect(DB_PATH, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def setup_db() -> None:
    """Initialize the PnL database schema."""
    with _get_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                size REAL NOT NULL,
                entry REAL NOT NULL,
                exit REAL NOT NULL,
                pnl REAL NOT NULL,
                cycle TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_fills_user_symbol on fills(user, symbol)
            """
        )
        conn.commit()
    logger.info("[PnLTracker] Database initialized at %s", DB_PATH)

# ────────────────────────────────────────────────────────────────────────────────
# PnL Data API
# ────────────────────────────────────────────────────────────────────────────────

def add_fill(
    symbol: str,
    side: str,
    size: float,
    entry: float,
    exit: float,
    pnl: float,
    user: str = "global",
    cycle: Optional[str] = None,
) -> None:
    """Insert a new trade fill record into the database."""
    try:
        with _get_conn() as conn:
            conn.execute(
                """
                INSERT INTO fills (user, symbol, side, size, entry, exit, pnl, cycle)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (user, symbol, side, size, entry, exit, pnl, cycle),
            )
            conn.commit()
        PNL_FILL_COUNT.inc()
    except Exception as e:
        PNL_FILL_ERRORS.inc()
        logger.error("[PnLTracker] Failed to add fill: %s", e)
        raise


def get_pnl_history(user: str = "global", symbol: Optional[str] = None) -> List[Dict]:
    """Fetch PnL history rows for a user and optional symbol."""
    start = datetime.now().timestamp()
    try:
        with _get_conn() as conn:
            if symbol:
                rows = conn.execute(
                    "SELECT * FROM fills WHERE user = ? AND symbol = ? ORDER BY timestamp",
                    (user, symbol),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM fills WHERE user = ? ORDER BY timestamp",
                    (user,),
                ).fetchall()
        data = [dict(row) for row in rows]
        return data
    finally:
        PNL_QUERY_LATENCY.observe(datetime.now().timestamp() - start)


def get_current_streak(user: str = "global") -> Optional[Dict[str, int]]:
    """Compute current consecutive wins or losses streak."""
    history = get_pnl_history(user)
    wins = losses = 0
    for row in reversed(history):
        if row['pnl'] > 0 and losses == 0:
            wins += 1
        elif row['pnl'] < 0 and wins == 0:
            losses += 1
        else:
            break
    return {'wins': wins, 'losses': losses} if (wins or losses) else None


def get_best_streak(user: str = "global") -> Optional[Dict[str, int]]:
    """Compute the longest win and loss streaks."""
    history = get_pnl_history(user)
    best = {'wins': 0, 'losses': 0}
    curr_w, curr_l = 0, 0
    for row in history:
        if row['pnl'] > 0:
            curr_w += 1
            curr_l = 0
        elif row['pnl'] < 0:
            curr_l += 1
            curr_w = 0
        best['wins'] = max(best['wins'], curr_w)
        best['losses'] = max(best['losses'], curr_l)
    return best if (best['wins'] or best['losses']) else None


def get_cycle_stats(user: str = "global") -> Optional[Dict[str, Any]]:
    """Fetch statistics for the most recent cycle: count, total PnL, win rate."""
    try:
        with _get_conn() as conn:
            row = conn.execute(
                """
                SELECT cycle, COUNT(*) AS trades, SUM(pnl) AS total_pnl,
                    SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) AS win_rate
                FROM fills
                WHERE user = ? AND cycle IS NOT NULL
                GROUP BY cycle
                ORDER BY cycle DESC
                LIMIT 1
                """,
                (user,),
            ).fetchone()
        return dict(row) if row else None
    except Exception as e:
        logger.error("[PnLTracker] get_cycle_stats error: %s", e)
        return None

async def generate_pnl_image(user: str = "global", symbol: Optional[str] = None) -> io.BytesIO:
    """Generate a PnL curve image for a user and optional symbol."""
    buf = io.BytesIO()
    start = datetime.now().timestamp()
    try:
        data = get_pnl_history(user, symbol)
        if not data:
            return buf
        pnl_values = [row['pnl'] for row in data]
        cumulative = [sum(pnl_values[:i+1]) for i in range(len(pnl_values))]

        plt.figure(figsize=(8, 4))
        plt.plot(cumulative, linewidth=2)
        plt.title(f"PnL Curve: {symbol or 'All'}")
        plt.xlabel("Trade #")
        plt.ylabel("Cumulative PnL")
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()
        plt.savefig(buf, format="png", dpi=300)
        plt.close()
        buf.seek(0)
    except Exception as e:
        PNL_IMAGE_ERRORS.inc()
        logger.error("[PnLTracker] generate_pnl_image error: %s", e)
    finally:
        PNL_IMAGE_LATENCY.observe(datetime.now().timestamp() - start)
        return buf

# End of PnL Tracker module
