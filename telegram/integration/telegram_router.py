# File: telegram/integration/telegram_router.py

import logging
from aiogram import Router, Bot, F, types
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from core.utils.config import config
from core.utils.plugin_manager import fire_plugin_event
from core.utils.pnl_card import generate_pnl_card
from core.utils.token_logo import fetch_token_logo
from core.utils.pnl_tracker import get_wallet_snapshot
from core.utils.trade_logger import get_trade_log, register_trade_log_hook
from core.signal_generation.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.signal_generation.combined_signal_engine import CombinedSignalEngine

from telegram.commands.commands import router as commands_router
from telegram.commands.pnl_commands import router as pnl_cmd_router
from telegram.commands.streak_cmd import router as streak_router

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Main router including sub-routers
router = Router()
router.include_router(commands_router)
router.include_router(pnl_cmd_router)
router.include_router(streak_router)

# Bot instance
bot = Bot(token=config.TELEGRAM_BOT_TOKEN)

# Admin-only decorator for callbacks
ADMIN_ID = str(config.TELEGRAM_CHAT_ID)
def admin_only(func):
    async def wrapper(query_or_message, *args, **kwargs):
        user_id = None
        if isinstance(query_or_message, Message):
            user_id = str(query_or_message.from_user.id)
        elif isinstance(query_or_message, CallbackQuery):
            user_id = str(query_or_message.from_user.id)
        if user_id != ADMIN_ID:
            # silent drop or notify
            if isinstance(query_or_message, Message):
                await query_or_message.reply("❌ Access denied.")
            else:
                await query_or_message.answer("❌ Access denied.")
            return
        return await func(query_or_message, *args, **kwargs)
    return wrapper

# Push closed-trade PnL cards
async def push_trade_pnl_card(event_name: str, data: dict):
    if event_name != "trade_logged":
        return
    symbol = data.get("symbol")
    pnl = data.get("pnl")
    side = data.get("side")
    img = await generate_pnl_card(symbol)
    caption = f"📊 Trade Closed: {symbol} ({side.upper()})\nPnL: {pnl:.4f}"
    await bot.send_photo(chat_id=int(config.TELEGRAM_CHAT_ID), photo=img, caption=caption)

register_trade_log_hook(push_trade_pnl_card)

# Push alpha signals with inline buttons
async def push_alpha_signal(event_name: str, data: dict):
    if event_name != "alpha_signal":
        return
    symbol = data.get("symbol")
    logo = await fetch_token_logo(symbol)
    caption = (
        f"🚨 *Nova Pulse Signal*\n"
        f"Token: {symbol}\n"
        f"Score: *{data.get('score',0):.2f}*\n"
        f"Entry: `{data.get('entry','N/A')}` TP: `{data.get('take_profit','...')}` SL: `{data.get('stop_loss','...')}`"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("🚀 Execute", callback_data=f"exec:{symbol}"),
         InlineKeyboardButton("👥 Mirror",   callback_data=f"mirror:{symbol}")],
        [InlineKeyboardButton("Auto ON/OFF", callback_data="auto_toggle")]
    ])
    await bot.send_photo(chat_id=int(config.TELEGRAM_CHAT_ID), photo=logo, caption=caption, parse_mode="Markdown", reply_markup=kb)

register_trade_log_hook(push_alpha_signal)

# Callback handlers for inline buttons
@router.callback_query(F.data.startswith("exec:"))
@admin_only
async def exec_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":",1)[1]
    await query.answer("🚀 Executing trade...")
    await fire_plugin_event("telegram_exec_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data == "auto_toggle")
@admin_only
async def auto_toggle_cb(query: CallbackQuery):
    await query.answer("🔁 Toggled auto mode")
    await fire_plugin_event("telegram_auto_toggle", {"user": query.from_user.id})

@router.callback_query(F.data.startswith("mirror:"))
@admin_only
async def mirror_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":",1)[1]
    await query.answer("👥 Mirroring trade")
    await fire_plugin_event("telegram_mirror_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data.startswith("skip:"))
@admin_only
async def skip_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":",1)[1]
    await query.answer("⏭️ Trade skipped")
    await fire_plugin_event("telegram_skip_trade", {"symbol": symbol, "user": query.from_user.id})

# Fallback status command (if needed)
@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    await message.answer(
        "🛰️ *Nova Pulse Status*\n"
        "Bot is running and connected!",
        parse_mode="Markdown"
    )
