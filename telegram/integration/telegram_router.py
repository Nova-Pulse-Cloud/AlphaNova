import logging
import asyncio
from aiogram import Router, Bot, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from core.utils.config import config
from core.utils.plugin_manager import fire_plugin_event, register_plugin_event_hook
from core.utils.pnl_card import generate_pnl_card
from core.utils.token_logo import fetch_token_logo
from core.utils.trade_logger import register_trade_log_hook
from core.signal_generation.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens

from telegram.commands.commands import router as commands_router
from telegram.commands.pnl_commands import router as pnl_cmd_router
from telegram.commands.streak_cmd import router as streak_router

logger = logging.getLogger("telegram_router")
logger.setLevel(logging.INFO)

router = Router()
router.include_router(commands_router)
router.include_router(pnl_cmd_router)
router.include_router(streak_router)

bot = Bot(token=config.TELEGRAM_BOT_TOKEN)

ADMIN_ID = str(config.TELEGRAM_CHAT_ID)

def admin_only(func):
    async def wrapper(obj, *args, **kwargs):
        user_id = str(obj.from_user.id) if hasattr(obj, 'from_user') else None
        if user_id != ADMIN_ID:
            if isinstance(obj, Message):
                await obj.reply("❌ Access denied. This bot is private.")
            elif isinstance(obj, CallbackQuery):
                await obj.answer("❌ Access denied. This bot is private.")
            return
        return await func(obj, *args, **kwargs)
    return wrapper

# ─── Trade PnL Image Push ──────────────────────────────────────────────
async def push_trade_pnl_card(event_name: str, data: dict):
    if event_name != "trade_logged":
        return
    symbol = data.get("symbol")
    pnl    = data.get("pnl")
    side   = data.get("side")
    img    = await generate_pnl_card(symbol)
    caption = f"📊 Trade Closed: {symbol} ({side.upper()})\nPnL: {pnl:.4f}"
    await bot.send_photo(chat_id=int(config.TELEGRAM_CHAT_ID), photo=img, caption=caption)

register_trade_log_hook(push_trade_pnl_card)

# ─── Signal Preview Push ────────────────────────────────────────────────
def _push_alpha_signal(event_name: str, data: dict):
    if event_name != "alpha_signal":
        return
    async def _send():
        try:
            symbol = data["symbol"]
            logo   = await fetch_token_logo(symbol)
            caption = (
                f"🚨 *Nova Pulse Signal*\n"
                f"Token: {symbol}\n"
                f"Score: *{data.get('score', 0):.2f}*\n"
                f"Entry: `{data.get('entry', 'N/A')}`\n"
                f"TP: `{data.get('take_profit', '...')}`\n"
                f"SL: `{data.get('stop_loss', '...')}`"
            )
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton("🚀 Execute", callback_data=f"exec:{symbol}"),
                 InlineKeyboardButton("👥 Mirror", callback_data=f"mirror:{symbol}")],
                [InlineKeyboardButton("🔁 Auto ON/OFF", callback_data="auto_toggle")]
            ])
            await bot.send_photo(
                chat_id=int(config.TELEGRAM_CHAT_ID),
                photo=logo,
                caption=caption,
                parse_mode="Markdown",
                reply_markup=kb
            )
        except Exception as e:
            logger.error(f"Failed to push alpha signal: {e}")

    asyncio.create_task(_send())

register_plugin_event_hook(_push_alpha_signal)

# ─── Inline Callback Handlers ───────────────────────────────────────────
@router.callback_query(F.data.startswith("exec:"))
@admin_only
async def exec_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":", 1)[1]
    await query.answer("🚀 Executing trade…")
    await fire_plugin_event("telegram_exec_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data.startswith("mirror:"))
@admin_only
async def mirror_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":", 1)[1]
    await query.answer("👥 Mirror trade activated!")
    await fire_plugin_event("telegram_mirror_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data == "auto_toggle")
@admin_only
async def auto_toggle_cb(query: CallbackQuery):
    await query.answer("🔁 Auto mode toggled.")
    await fire_plugin_event("telegram_auto_toggle", {"user": query.from_user.id})

@router.callback_query(F.data.startswith("skip:"))
@admin_only
async def skip_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":", 1)[1]
    await query.answer("⏭️ Trade skipped.")
    await fire_plugin_event("telegram_skip_trade", {"symbol": symbol, "user": query.from_user.id})

# ─── Status Check ───────────────────────────────────────────────────────
@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    await message.answer("🛰️ Nova Pulse Status: ✅ Running and connected")
