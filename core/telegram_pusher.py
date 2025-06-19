import logging
import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pydantic_settings import BaseSettings

from core.utils.pnl_card import generate_pnl_card

# === Settings & Bot Init ===
class Settings(BaseSettings):
    TELEGRAM_BOT_TOKEN: str
    TELEGRAM_CHAT_ID: int
    DASHBOARD_URL: str  # URL to your public dashboard for detailed stats

    class Config:
        env_file = ".env"

settings = Settings()
bot = Bot(token=settings.TELEGRAM_BOT_TOKEN)
logger = logging.getLogger("core.telegram_pusher")

async def push_trade_update(trade: dict):
    """
    Sends a detailed post-trade update to Telegram with a PnL card and
    actionable inline buttons linking to the dashboard or more details.
    """
    try:
        symbol     = trade.get("symbol")
        exec_data  = trade.get("execution", {})
        side       = exec_data.get("side", "N/A")
        size       = exec_data.get("size", "N/A")
        entry      = exec_data.get("entry", "N/A")
        exit_price = exec_data.get("exit", "N/A")
        pnl        = exec_data.get("pnl", 0.0)
        success    = exec_data.get("success", False)

        caption = (
            f"📊 *Trade Update*
"
            f"Symbol: `{symbol}`
"
            f"Side: *{side}* | Size: `{size}`
"
            f"Entry: `{entry}` | Exit: `{exit_price}`
"
            f"PnL: `{pnl:+.4f}`
"
            f"Status: {'✅ Success' if success else '❌ Failed'}"
        )

        # Generate the PnL card image
        img = await generate_pnl_card(symbol)

        # Build inline keyboard
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("View Dashboard", url=settings.DASHBOARD_URL),
            InlineKeyboardButton("Trade Details", callback_data=f"trade_details:{symbol}")
        )

        # Send photo with caption and buttons
        await bot.send_photo(
            chat_id=settings.TELEGRAM_CHAT_ID,
            photo=img,
            caption=caption,
            reply_markup=keyboard,
            parse_mode="Markdown"
        )

    except Exception as e:
        logger.error("[push_trade_update] %s", e)
