import io
import logging
import matplotlib.pyplot as plt
import pandas as pd
from core.utils.trade_memory import get_trade_history_by_user_or_symbol

logger = logging.getLogger("pnl_chart")

async def generate_pnl_image(user: str = None, symbol: str = None) -> io.BytesIO:
    """
    Creates an equity curve chart based on trade history.
    Returns in-memory PNG buffer.

    :param user: Optional user identifier for fetching trade history
    :param symbol: Optional symbol for fetching trade history
    :return: In-memory PNG buffer containing the PnL chart
    """
    try:
        # Fetch the trade history
        trades = await get_trade_history_by_user_or_symbol(user, symbol)
        if not trades:
            logger.warning(f"No trades found for user: {user}, symbol: {symbol}")
            return None
        
        # Prepare the data frame
        df = pd.DataFrame([t.dict() for t in trades])
        df = df.sort_values("timestamp")
        df["equity"] = df["outcome"].cumsum()

        # Generate the plot
        fig, ax = plt.subplots(figsize=(10, 5))  # Improved figure size for better readability
        df["equity"].plot(ax=ax, title=f"PnL Curve {'for ' + symbol if symbol else 'for User ' + user}")
        ax.set_xlabel("Trades")
        ax.set_ylabel("Cumulative PnL")
        ax.grid(True)

        # Create in-memory PNG buffer
        buf = io.BytesIO()
        plt.savefig(buf, format="PNG", dpi=300)  # High-resolution image
        plt.close(fig)
        buf.seek(0)
        return buf

    except Exception as e:
        logger.exception(f"[pnl_chart] Failed to generate equity chart for user: {user}, symbol: {symbol}. Error: {e}")
        return None
