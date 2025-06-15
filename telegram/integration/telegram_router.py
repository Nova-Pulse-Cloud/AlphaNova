# File: telegram/integration/telegram_router.py

import logging
from aiogram import Router, types, Bot, F
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
from core.signal_generation.signal_engine_v3 import get_latest_signal  # assumed
from core.signal_generation.combined_signal_engine import CombinedSignalEngine  # assumed

from telegram.commands.commands import router as commands_router
from telegram.commands.pnl_commands import router as pnl_cmd_router
from telegram.commands.streak_cmd import router as streak_router

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
router = Router()
router.include_router(commands_router)
router.include_router(pnl_cmd_router)
router.include_router(streak_router)

bot = Bot(token=config.TELEGRAM_BOT_TOKEN)

def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if str(message.from_user.id) != str(config.TELEGRAM_CHAT_ID):
            await message.reply("❌ Access denied. This bot is private.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

@router.message(Command(commands=["start", "help"]))
@admin_only
async def start_handler(message: Message):
    welcome = (
        "👋 *Welcome to Nova Pulse!*\n"
        "🚀 The world's most advanced AI crypto trading bot.\n\n"
        "*Live Commands:*\n"
        "/wallet — Show current wallet balance\n"
        "/pnl — Show PnL (w/ card)\n"
        "/logs — Show last 20 trades\n"
        "/signalcard <symbol> — Generate a signal card\n"
        "/stats <symbol> — Token memory stats\n"
        "/social — Trending tokens\n"
        "/whales — Whale swaps\n"
        "/alpha — Ranked alpha signals\n"
        "/explain <symbol> — AI signal breakdown\n"
        "/streak — Win/loss streak\n"
        "/auto — Toggle auto-trading\n"
        "/mode <degen/moderate/safe> — Set trading mode\n"
        "/mirror <symbol> — Mirror top wallet\n"
        "/exportdata — Export all fills as CSV\n"
        "/status — Bot/auto-trading status\n"
    )
    await message.answer(welcome, parse_mode="Markdown")

@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /explain <SYMBOL>")
        return
    symbol = args[1].upper()
    ai = SignalExplainerAI()
    result = await ai.explain({"symbol": symbol, "components": {}})
    narrative = result.narrative if hasattr(result, "narrative") else result.get("narrative", "No explanation available.")
    confidence = result.confidence if hasattr(result, "confidence") else result.get("confidence", 0.0)
    alert = result.alert if hasattr(result, "alert") else result.get("alert", False)
    msg = f"🧠 *{symbol} AI Insight:*\n{narrative}\n_Confidence: {confidence:.2f}_"
    if alert:
        msg += "\n🚨 *High-confidence alert!*"
    await message.answer(msg, parse_mode="Markdown")
    await fire_plugin_event("cmd_explain_signal", {"symbol": symbol, "narrative": narrative, "confidence": confidence, "alert": alert})

def mirror_trade_buttons(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Mirror Trade", callback_data=f"mirror:{symbol}")],
        [InlineKeyboardButton(text="📈 View Leaderboard", url=config.DASHBOARD_URL)]
    ])

def auto_toggle_buttons(is_auto: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🟢 Auto ON" if not is_auto else "🔴 Auto OFF",
                              callback_data="auto_toggle")]
    ])

def exec_trade_buttons(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Execute Trade", callback_data=f"exec:{symbol}")],
        [InlineKeyboardButton(text="📉 Skip", callback_data=f"skip:{symbol}")]
    ])

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

async def push_signal_card(symbol: str, signal: dict):
    caption = (
        f"🚨 *Nova Pulse Signal*\n"
        f"Token: ${symbol}\n"
        f"Entry: `{signal.get('entry', 'N/A')}`\n"
        f"TP: `{signal.get('tp', 'N/A')}` | SL: `{signal.get('sl', 'N/A')}`\n"
        f"Score: *{signal.get('score', 0):.2f}* | Flavor: *{signal.get('flavor', '...')}*\n"
        f"{signal.get('caption', '')}\n"
        f"\n[SignalCard powered by Nova Pulse AI]"
    )
    logo = await fetch_token_logo(symbol)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton("🚀 Execute", callback_data=f"exec:{symbol}")],
            [InlineKeyboardButton("👥 Mirror", callback_data=f"mirror:{symbol}")],
            [InlineKeyboardButton("Auto ON/OFF", callback_data="auto_toggle")],
        ]
    )
    await bot.send_photo(chat_id=int(config.TELEGRAM_CHAT_ID), photo=logo, caption=caption, parse_mode="Markdown", reply_markup=kb)

@router.callback_query(F.data.startswith("exec:"))
async def exec_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":")[1]
    await query.answer("🚀 Executing trade...")
    await query.message.edit_reply_markup(reply_markup=None)
    await fire_plugin_event("telegram_exec_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data == "auto_toggle")
async def auto_toggle_cb(query: CallbackQuery):
    await query.answer("🔁 Auto mode toggled.")
    await query.message.edit_reply_markup(reply_markup=None)
    await fire_plugin_event("telegram_auto_toggle", {"user": query.from_user.id})

@router.callback_query(F.data.startswith("mirror:"))
async def mirror_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":")[1]
    await query.answer("👥 Mirror trading activated!")
    await fire_plugin_event("telegram_mirror_trade", {"symbol": symbol, "user": query.from_user.id})

@router.callback_query(F.data.startswith("skip:"))
async def skip_trade_cb(query: CallbackQuery):
    symbol = query.data.split(":")[1]
    await query.answer("⏭️ Trade skipped.")
    await query.message.edit_reply_markup(reply_markup=None)
    await fire_plugin_event("telegram_skip_trade", {"symbol": symbol, "user": query.from_user.id})

@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    auto_status = "ON"  # TODO: Wire to actual Redis/config
    msg = (
        f"🛰️ *Nova Pulse Status*\n"
        f"Auto-Trading: `{auto_status}`\n"
        f"Last signal: ...\n"
        f"Total PnL: ...\n"
        f"Bot is running and connected!"
    )
    await message.answer(msg, parse_mode="Markdown")

async def push_alpha_signal_event(event_name: str, data: dict):
    if event_name != "alpha_signal":
        return
    await push_signal_card(data.get("symbol", "?"), data)

register_trade_log_hook(push_alpha_signal_event)

# --- End of file ---
