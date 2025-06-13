# 🚀 Nova Pulse ALPHA – telegram/telegram_router.py
# Advanced aiogram-based command router with modular, extensible sub-routers for all advanced Alpha+ features.

import os
import logging
from aiogram import Router, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from core.plugin_manager import fire_plugin_event  # Centralized event firing for integration

from utils.pnl_image_card import generate_pnl_card
from core.token_logo import fetch_token_logo
from core.pnl_tracker import get_wallet_snapshot
from core.trade_logger import get_trade_log, register_trade_log_hook
from core.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_explainer_ai import SignalExplainerAI
from utils.config import TELEGRAM_BOT_TOKEN, REDIS_URL, OPENAI_API_KEY

from telegram.commands import router as commands_router
from telegram.pnl_commands import router as pnl_cmd
from telegram.streak_cmd import router as streak_router

logger = logging.getLogger("telegram_router")

# === Main Telegram Router ===
router = Router()
router.include_router(commands_router)
router.include_router(pnl_cmd)
router.include_router(streak_router)

# === Admin Access Control ===
ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "7545342838"))

def admin_only(func):
    """
    Decorator to ensure that only the admin can execute the commands.
    """
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

# === Start and Help Command ===
@router.message(Command(commands=["start", "help"]))
@admin_only
async def start_handler(message: Message):
    """
    Displays the welcome message and available commands.
    """
    welcome = (
        "👋 *Welcome to Nova Pulse!*\n"
        "🚀 The most advanced AI crypto trading bot.\n\n"
        "*Live Commands:*\n"
        "/wallet — Show current wallet balance\n"
        "/pnl — Show unrealized and realized PnL (with image card)\n"
        "/logs — Display the last 20 trade logs\n"
        "/signalcard <mint> — Generate an AI-powered PnL card for a token\n"
        "/stats <SYMBOL> — Memory stats for a token (cycle, win rate, etc.)\n"
        "/social — Top trending tokens on Twitter/X\n"
        "/whales — Latest whale wallet token swaps\n"
        "/alpha — Ranked token alpha from Nova’s AI Engine\n"
        "/explain <SYMBOL> — Explain signal with AI caption\n"
        "/streak — Track your win/loss streak history\n"
        "/start — Show this command list again"
    )
    await message.answer(welcome, parse_mode="Markdown")

# === Explain Command ===
@router.message(Command("explain"))
@admin_only
async def explain_signal_handler(message: Message):
    """
    Generates an AI explanation for a given token's signal.
    """
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer("❗ Usage: /explain <SYMBOL>")
            return

        symbol = args[1].upper()
        ai = SignalExplainerAI()
        explanation_result = await ai.explain_signal({"symbol": symbol, "components": {}})

        narrative = explanation_result.get("narrative") or "No explanation available."
        alert = explanation_result.get("alert", False)
        confidence = explanation_result.get("confidence")

        msg = f"🧠 *{symbol} AI Insight:*\n\n{narrative}\n\nConfidence: {confidence:.2f}"
        if alert:
            msg += "\n🚨 *High-confidence alert!*"

        await message.answer(msg, parse_mode="Markdown")
        await fire_plugin_event("cmd_explain_signal", {"symbol": symbol, "narrative": narrative, "confidence": confidence, "alert": alert})

    except Exception as e:
        logger.error(f"[ExplainHandler] Error: {e}")
        await message.answer(f"⚠️ Failed to generate explanation: {e}")

# === Mirror Trade Inline Buttons ===
def mirror_trade_buttons(symbol: str) -> InlineKeyboardMarkup:
    """
    Provides inline buttons for mirroring trades and viewing leaderboard.
    """
    return InlineKeyboardMarkup(inline_keyboard=[ 
        [InlineKeyboardButton(text="👥 Mirror Trade", callback_data=f"mirror:{symbol}")],
        [InlineKeyboardButton(text="📈 View on Leaderboard", url="https://nova-pulse.ai/leaderboard")]
    ])

# === Register Hook for PnL Card on Trade Close ===
from aiogram import Bot
bot = Bot(token=TELEGRAM_BOT_TOKEN)

async def push_trade_pnl_card(event_name: str, data: dict):
    """
    Fires when a trade is logged. Sends the PnL card for the trade to the admin.
    """
    if event_name != "trade_logged":
        return
    try:
        symbol = data.get("symbol", "N/A")
        side = data.get("side")
        pnl = data.get("pnl")
        entry = data.get("entry")
        exit_price = data.get("exit")
        size = data.get("size")

        img_bytes = await generate_pnl_card(
            symbol=symbol,
            entry=entry,
            exit_price=exit_price,
            pnl=pnl,
            size=size,
            side=side,
            timestamp=data.get("timestamp")
        )
        await bot.send_photo(chat_id=ADMIN_CHAT_ID, photo=img_bytes, caption=f"📊 Trade Closed: {symbol} ({side.upper()})\nPnL: {pnl:.4f}")
    except Exception as e:
        logger.error(f"[PNL_CARD_SEND_ERROR] {e}")

# === Register the Trade Log Hook ===
register_trade_log_hook(push_trade_pnl_card)
