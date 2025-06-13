import os
import sqlite3
import matplotlib.pyplot as plt
from datetime import datetime
from typing import List, Dict, Optional
from io import BytesIO
import logging

logger = logging.getLogger("pnl_tracker")

DB_PATH = os.getenv("PNL_DB_PATH", "pnl_tracker.db")

def _get_conn():
    """Get a connection to the SQLite database with safe parameterized queries."""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Return rows as dictionaries
        return conn
    except sqlite3.DatabaseError as e:
        logger.error(f"Database connection error: {e}")
        raise

def setup_db():
    """Initialize the database with the required tables and indexes."""
    try:
        with _get_conn() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS fills (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT,
                symbol TEXT,
                side TEXT,
                size REAL,
                entry REAL,
                exit REAL,
                pnl REAL,
                cycle TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """)
            conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_fills_user_symbol ON fills(user, symbol)
            """)
            conn.commit()
    except sqlite3.DatabaseError as e:
        logger.error(f"Error setting up database: {e}")

def add_fill(symbol: str, side: str, size: float, entry: float, exit: float, pnl: float, user: str = "global", cycle: Optional[str] = None):
    """Add a new fill (trade) entry into the database."""
    try:
        with _get_conn() as conn:
            conn.execute("""
            INSERT INTO fills (user, symbol, side, size, entry, exit, pnl, cycle)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (user, symbol, side, size, entry, exit, pnl, cycle))
            conn.commit()
    except sqlite3.DatabaseError as e:
        logger.error(f"Error inserting fill data: {e}")

def get_pnl_history(user: str = "global", symbol: Optional[str] = None) -> List[Dict]:
    """Fetch the trade history and PnL data for a given user and symbol."""
    try:
        with _get_conn() as conn:
            query = "SELECT * FROM fills WHERE user=?"
            params = [user]
            if symbol:
                query += " AND symbol=? ORDER BY timestamp"
                params.append(symbol)
            else:
                query += " ORDER BY timestamp"
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]
    except sqlite3.DatabaseError as e:
        logger.error(f"Error fetching PnL history: {e}")
        return []

def get_streak_history(user: str = "global") -> Optional[Dict]:
    """Fetch the current streak (wins and losses) for a user."""
    data = get_pnl_history(user)
    streak = {"wins": 0, "losses": 0}
    for row in reversed(data):
        if row["pnl"] > 0:
            if streak["losses"] > 0: break
            streak["wins"] += 1
        elif row["pnl"] < 0:
            if streak["wins"] > 0: break
            streak["losses"] += 1
    return streak if (streak["wins"] or streak["losses"]) else None

def get_best_streak(user: str = "global") -> Optional[Dict]:
    """Fetch the best streak (longest wins or losses) for a user."""
    data = get_pnl_history(user)
    best = {"wins": 0, "losses": 0}
    win, loss = 0, 0
    for row in data:
        if row["pnl"] > 0:
            win += 1
            loss = 0
        elif row["pnl"] < 0:
            loss += 1
            win = 0
        best["wins"] = max(best["wins"], win)
        best["losses"] = max(best["losses"], loss)
    return best if (best["wins"] or best["losses"]) else None

def get_cycle_stats(user: str = "global") -> Optional[Dict]:
    """Fetch the most recent cycle stats, including win rate and total PnL."""
    try:
        with _get_conn() as conn:
            row = conn.execute("""
            SELECT cycle, COUNT(*) as trades, SUM(pnl) as pnl,
                SUM(CASE WHEN pnl>0 THEN 1 ELSE 0 END)*1.0/COUNT(*) as win_rate
            FROM fills WHERE user=? GROUP BY cycle ORDER BY cycle DESC LIMIT 1
            """, (user,)).fetchone()
        if not row or not row["cycle"]:
            return None
        return dict(row)
    except sqlite3.DatabaseError as e:
        logger.error(f"Error fetching cycle stats: {e}")
        return None

async def generate_pnl_image(user: str = "global", symbol: Optional[str] = None) -> BytesIO:
    """Generate and return a PnL curve image for the user and symbol."""
    try:
        data = get_pnl_history(user, symbol)
        if not data: return None
        pnl_curve = [row['pnl'] for row in data]
        cumsum = [sum(pnl_curve[:i+1]) for i in range(len(pnl_curve))]

        plt.figure(figsize=(6, 3))
        plt.plot(cumsum, linewidth=3)
        plt.title(f"PnL Curve ({symbol if symbol else 'All'})")
        plt.xlabel("Trade #")
        plt.ylabel("PnL (USD)")
        plt.grid(True, linestyle="--", alpha=0.5)
        plt.tight_layout()
        
        buf = BytesIO()
        plt.savefig(buf, format="png", dpi=300)  # High-quality output
        plt.close()
        buf.seek(0)
        return buf
    except Exception as e:
        logger.error(f"Failed to generate PnL image for user: {user}, symbol: {symbol}. Error: {e}")
        return None

# ----- Future Enhancements: Animated GIF equity curves, leaderboard overlays, challenge modes -----
