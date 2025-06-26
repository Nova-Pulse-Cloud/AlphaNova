# File: telegram/commands/pnl_commands.py
# 🚀 Nova Pulse ALPHA – Elite PnL Command Router
# Full-spectrum async PnL commands: compressed card, equity curve, symbol/cycle stats, and mesh-aware LLM captions.

import os
import logging
import re
import uuid
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from utils.pnl_renderer import generate_pnl_image, generate_pnl_card, get_pnl_history, get_token_stats
from utils.plugin_manager import fire_plugin_event
from core.signal_generation.reinforcement_learning import RLTrader
from telegram.commands.commands import COMMANDS_EXECUTED, COMMANDS_FAILED

logger = logging.getLogger("telegram_pnl_commands")
router = Router()

ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "7545342838"))

def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper

def sanitize_symbol(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", text.upper())

# === /pnl — Returns a high-res image of all PnL (or symbol if provided) ===
@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    symbol = sanitize_symbol(args[1]) if len(args) > 1 else None
    trace_id = f"pnl-{uuid.uuid4().hex[:6]}"
    try:
        img = await generate_pnl_image(user=user_id, symbol=symbol)
        caption = f"📈 PnL {'for ' + symbol if symbol else 'overview'}"
        stats = _mesh.get_token_stats(symbol)
        if stats and "cycle" in stats:
            caption += f"\n⏳ Cycle: {stats['cycle']} | Win Rate: {stats.get('win_rate', 'N/A')}"
        await fire_plugin_event("cmd_pnl", {"user_id": user_id, "symbol": symbol, "trace_id": trace_id})
        await message.answer_photo(img, caption=caption)
        logger.info(f"[PnL] Sent PnL image to user {user_id} for symbol={symbol}")
    except Exception as e:
        logger.error(f"[PnL] Failed to generate PnL image: {e}")
        await message.answer("⚠️ Error generating PnL image.")
    COMMANDS_EXECUTED.inc()

# === /pnlcard <symbol> — Visual single-token PnL card ===
@router.message(Command("pnlcard"))
@admin_only
async def pnlcard_handler(message: Message):
    args = message.text.split()
    symbol = sanitize_symbol(args[1]) if len(args) > 1 else None
    trace_id = f"pnlcard-{uuid.uuid4().hex[:6]}"
    if not symbol:
        await message.answer("❗ Usage: /pnlcard <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    try:
        card = await generate_pnl_card(symbol)
        await fire_plugin_event("cmd_pnl_card", {"symbol": symbol, "trace_id": trace_id})
        await message.answer_photo(card, caption=f"🃏 PnL Card for {symbol}")
        logger.info(f"[PnLCard] Sent PnL card for symbol={symbol} to user={message.from_user.id}")
    except Exception as e:
        logger.error(f"[PnLCard] Failed for symbol={symbol}: {e}")
        await message.answer(f"❗ Could not generate card for {symbol}")
    COMMANDS_EXECUTED.inc()

# === /pnlsummary [symbol] — Returns stats summary (total, avg, winrate, streak) ===
@router.message(Command("pnlsummary"))
@admin_only
async def pnlsummary_handler(message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    symbol = sanitize_symbol(args[1]) if len(args) > 1 else None
    trace_id = f"pnlsummary-{uuid.uuid4().hex[:6]}"
    try:
        pnl_data = await get_pnl_history(user=user_id, symbol=symbol)
        if pnl_data:
            total = sum(item.get('pnl', 0) for item in pnl_data)
            n = len(pnl_data)
            avg = total / n if n > 0 else 0
            win = sum(1 for item in pnl_data if item.get('pnl', 0) > 0)
            streak = 0
            for t in reversed(pnl_data):
                if t.get('pnl', 0) > 0: streak += 1
                else: break
            msg = (
                f"📊 PnL summary for {symbol or 'all'}:\n"
                f"Total: {total:.2f}\n"
                f"Trades: {n}\n"
                f"Avg: {avg:.2f}\n"
                f"Winrate: {100 * win / n:.1f}%\n"
                f"Win streak: {streak}"
            )
            await message.answer(msg)
            await fire_plugin_event(
                "cmd_pnl_summary",
                {"user_id": user_id, "symbol": symbol, "total": total, "avg": avg, "win": win, "streak": streak, "trace_id": trace_id}
            )
            logger.info(f"[PnLSummary] Sent summary to user {user_id} for symbol={symbol}")
        else:
            await message.answer("📭 No trade data available for summary.")
    except Exception as e:
        logger.error(f"[PnLSummary] Error: {e}")
        await message.answer("⚠️ Failed to fetch PnL summary.")
    COMMANDS_EXECUTED.inc()

# === /pnlchart — equity curve as chart ===
@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    user_id = message.from_user.id
    trace_id = f"pnlchart-{uuid.uuid4().hex[:6]}"
    try:
        img = await generate_pnl_image(user=user_id)
        await fire_plugin_event("cmd_pnl_chart", {"user_id": user_id, "trace_id": trace_id})
        await message.answer_photo(img, caption="📊 Account Equity Curve")
        logger.info(f"[PnLChart] Equity curve sent to user {user_id}")
    except Exception as e:
        logger.error(f"[PnLChart] Error: {e}")
        await message.answer("⚠️ Failed to generate equity curve.")
    COMMANDS_EXECUTED.inc()
