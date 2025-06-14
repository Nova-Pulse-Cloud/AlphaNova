# File: telegram/integration/telegram_router.py

import os
import logging
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from utils.config import Config
from utils.plugin_manager import fire_plugin_event
from utils.pnl_card import generate_pnl_card
from utils.token_logo import fetch_token_logo
from utils.pnl_tracker import get_wallet_snapshot
from utils.trade_logger import get_trade_log, register_trade_log_hook
from core.signal_generation.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_generation.signal_explainer_ai import SignalExplainerAI

# Import command routers
from telegram.commands.commands import router as commands_router
from telegram.commands.pnl_commands import router as pnl_cmd_router
from telegram.commands.streak_cmd import router as streak_router

# Initialize config and logger
config = Config()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Main Telegram Router
router = Router()
router.include_router(commands_router)
router.include_router(pnl_cmd_router)
router.include_router(streak_router)

# Admin control
def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != int(config.TELEGRAM_CHAT_ID):
            await message.reply("❌ Access denied. This bot is private.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# /start & /help
@router.message(Command(commands=["start", "help"]))
@admin_only
async def start_handler(message: Message):
    welcome = (
        "👋 *Welcome to Nova Pulse!*\n"
        "🚀 The most advanced AI crypto trading bot.\n\n"
        "*Live Commands:*\n"
        "/wallet — Show current wallet balance\n"
        "/pnl — Show unrealized and realized PnL (with image card)\n"
        "/logs — Display the last 20 trade logs\n"
        "/signalcard <symbol> — Generate a signal card image\n"
        "/stats <symbol> — Memory stats (cycle, win rate)\n"
        "/social — Top trending tokens\n"
        "/whales — Recent whale swaps\n"
        "/alpha — Ranked alpha signals\n"
        "/explain <symbol> — AI explanation of signal\n"
        "/streak — Win/loss streak history\n"
    )
    await message.answer(welcome, parse_mode="Markdown")

# /explain
@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /explain <SYMBOL>")
        return
    symbol = args[1].upper()
    ai = SignalExplainerAI()
    result = await ai.explain_signal({"symbol": symbol, "components": {}})
    narrative = result.get("narrative", "No explanation available.")
    confidence = result.get("confidence", 0.0)
    alert = result.get("alert", False)
    msg = f"🧠 *{symbol} AI Insight:*\n{narrative}\n_Confidence: {confidence:.2f}_"
    if alert:
        msg += "\n🚨 *High-confidence alert!*"
    await message.answer(msg, parse_mode="Markdown")
    fire_plugin_event("cmd_explain_signal", {"symbol": symbol, "narrative": narrative, "confidence": confidence, "alert": alert})

# Inline buttons helper
def mirror_trade_buttons(symbol: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👥 Mirror Trade", callback_data=f"mirror:{symbol}" )],
        [InlineKeyboardButton(text="📈 View Leaderboard", url=config.DASHBOARD_URL)]
    ])

# Push PnL card on trade log events
from aiogram import Bot
bot = Bot(token=config.TELEGRAM_BOT_TOKEN)

async def push_trade_pnl_card(event_name: str, data: dict):
    if event_name != "trade_logged":
        return
    symbol = data.get("symbol")
    entry = data.get("entry")
    exit_ = data.get("exit")
    pnl = data.get("pnl")
    side = data.get("side")
    img = await generate_pnl_card(symbol)
    caption = f"📊 Trade Closed: {symbol} ({side.upper()})\nPnL: {pnl:.4f}"
    await bot.send_photo(chat_id=int(config.TELEGRAM_CHAT_ID), photo=img, caption=caption)

# Register log hook
register_trade_log_hook(push_trade_pnl_card)
