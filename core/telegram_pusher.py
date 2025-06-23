import logging
import asyncio
from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from pydantic_settings import BaseSettings

from core.utils.signal_card import generate_signal_card
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

async def push_signal_card(fused: dict):
    """
    Sends a signal card to Telegram when a new trading signal is generated.
    """
    try:
        symbol = fused.get("symbol")
        img = await generate_signal_card(fused)
        caption = (
            f"🚨 *Nova Pulse Signal*\n"
            f"Token: `{symbol}`\n"
            f"Score: *{fused.get('score', 0):.2f}*\n"
            f"Entry: `{fused.get('entry', 'N/A')}`"
        )
        keyboard = InlineKeyboardMarkup(row_width=2)
        keyboard.add(
            InlineKeyboardButton("🚀 Execute", callback_data=f"exec:{symbol}"),
            InlineKeyboardButton("👥 Mirror", callback_data=f"mirror:{symbol}"),
        )
        keyboard.add(
            InlineKeyboardButton("🔁 Auto ON/OFF", callback_data="auto_toggle")
        )
        await bot.send_photo(
            chat_id=settings.TELEGRAM_CHAT_ID,
            photo=img,
            caption=caption,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error("[push_signal_card] %s", e)

async def push_trade_card(fused: dict, exec_res: dict):
    """
    Sends a brief execution card to Telegram after a trade is executed.
    """
    try:
        symbol     = fused.get("symbol")
        side       = exec_res.get("side", "N/A").upper()
        size       = exec_res.get("size", "N/A")
        entry      = exec_res.get("entry", "N/A")
        exit_price = exec_res.get("exit", "N/A")
        success    = exec_res.get("success", False)
        caption = (
            f"💹 *Trade Executed*\n"
            f"Symbol: `{symbol}` | Side: *{side}*\n"
            f"Size: `{size}` | Entry: `{entry}` | Exit: `{exit_price}`\n"
            f"Status: {'✅ Success' if success else '❌ Failed'}"
        )
        keyboard = InlineKeyboardMarkup(row_width=1)
        keyboard.add(
            InlineKeyboardButton("View Dashboard", url=settings.DASHBOARD_URL)
        )
        await bot.send_message(
            chat_id=settings.TELEGRAM_CHAT_ID,
            text=caption,
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    except Exception as e:
        logger.error("[push_trade_card] %s", e)

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
            f"📊 *Trade Update*\n"
            f"Symbol: `{symbol}`\n"
            f"Side: *{side}* | Size: `{size}`\n"
            f"Entry: `{entry}` | Exit: `{exit_price}`\n"
            f"PnL: `{pnl:+.4f}` | Status: {'✅ Success' if success else '❌ Failed'}"
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
            parse_mode="Markdown",
            reply_markup=keyboard
        )

    except Exception as e:
        logger.error("[push_trade_update] %s", e)

async def push_pnl_card(symbol: str):
    """
    Sends a standalone PnL card for closed trades.
    """
    try:
        img = await generate_pnl_card(symbol)
        caption = f"📈 PnL Snapshot for `{symbol}`"
        await bot.send_photo(
            chat_id=settings.TELEGRAM_CHAT_ID,
            photo=img,
            caption=caption,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.error("[push_pnl_card] %s", e)
