# 🚀 Nova Pulse ALPHA – telegram/pnl_commands.py
# Elite-tier PnL command router (aiogram)—image-first, extensible, and Alpha Engine ready.
# Fully async, compressed image responses, LLM/mesh-aware captions, and telemetry.

import os
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
import logging
import re
import asyncio

from utils.pnl_renderer import generate_pnl_image, generate_pnl_card, get_pnl_history, get_token_stats
from core.plugin_manager import fire_plugin_event
from core.reinforcement_learning import RLTrader  # Adding RL trader feedback

logger = logging.getLogger("telegram_pnl_commands")
router = Router()

# === Config ===
ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "7545342838"))

def admin_only(func):
    """
    Decorator to ensure that only the admin can access the commands.
    """
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            return
        return await func(message, *args, **kwargs)
    return wrapper

def sanitize_symbol(text: str) -> str:
    """
    Sanitize input symbol to ensure no invalid characters.
    """
    return re.sub(r"[^A-Za-z0-9]", "", text.upper())

@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    """
    Handler for fetching the PnL image for the user or specific symbol.
    """
    user_id = message.from_user.id
    args = message.text.split()
    symbol = sanitize_symbol(args[1]) if len(args) > 1 else None
    trace_id = f"pnl-{uuid.uuid4().hex[:6]}"

    try:
        img = await generate_pnl_image(user=user_id, symbol=symbol)
        caption = f"📈 PnL {f'for {symbol}' if symbol else ''}"
        stats = await get_token_stats(symbol) if symbol else None

        # Add extra info to caption if available
        if stats and "cycle" in stats:
            caption += f"\n⏳ Cycle: {stats['cycle']} | Win Rate: {stats.get('win_rate', 'N/A')}"

        # Fire event and RL feedback
        await fire_plugin_event("cmd_pnl", {"user_id": user_id, "symbol": symbol, "trace_id": trace_id})

        await message.answer_photo(img, caption=caption)
        logger.info(f"[PnL] Sent PnL image to user {user_id} for symbol={symbol}")
    except Exception as e:
        logger.error(f"[PnL] Failed to generate PnL image: {e}")
        await message.answer("⚠️ Error generating PnL image.")

    COMMANDS_EXECUTED.inc()

@router.message(Command("pnlcard"))
@admin_only
async def pnlcard_handler(message: Message):
    """
    Handler for generating the PnL card for the provided mint symbol.
    """
    args = message.text.split()
    mint = args[1] if len(args) > 1 else None
    trace_id = f"pnlcard-{uuid.uuid4().hex[:6]}"

    if not mint:
        await message.answer("❗ Usage: /pnlcard <mint>")
        COMMANDS_FAILED.inc()
        return

    try:
        card = await generate_pnl_card(mint)
        await fire_plugin_event("cmd_pnl_card", {"mint": mint, "trace_id": trace_id})
        await message.answer_photo(card, caption=f"🃏 PnL Card for {mint}")
        logger.info(f"[PnLCard] Sent PnL card for mint={mint} to user={message.from_user.id}")
    except Exception as e:
        logger.error(f"[PnLCard] Failed for mint={mint}: {e}")
        await message.answer(f"❗ Could not generate card for {mint}")

    COMMANDS_EXECUTED.inc()

@router.message(Command("pnlsummary"))
@admin_only
async def pnlsummary_handler(message: Message):
    """
    Handler for sending a summary of the PnL for the user or a symbol.
    """
    user_id = message.from_user.id
    trace_id = f"pnlsummary-{uuid.uuid4().hex[:6]}"
    args = message.text.split()
    symbol = sanitize_symbol(args[1]) if len(args) > 1 else None

    try:
        pnl_data = await get_pnl_history(user=user_id, symbol=symbol)
        if pnl_data:
            total = sum([item['pnl'] for item in pnl_data])
            n = len(pnl_data)
            avg = total / n if n > 0 else 0
            msg = f"📊 PnL summary for {symbol or 'all'}:\nTotal: {total:.2f}\nTrades: {n}\nAvg: {avg:.2f}"
            await message.answer(msg)

            # Fire event with RL feedback
            await fire_plugin_event("cmd_pnl_summary", {"user_id": user_id, "symbol": symbol, "total": total, "avg": avg, "trace_id": trace_id})
            logger.info(f"[PnLSummary] Sent summary to user {user_id} for symbol={symbol}")
        else:
            await message.answer("📭 No trade data available for summary.")

    except Exception as e:
        logger.error(f"[PnLSummary] Error: {e}")
        await message.answer("⚠️ Failed to fetch PnL summary.")

    COMMANDS_EXECUTED.inc()

# === Future Plug-ins ===
# /pnlcompare <token1> <token2> — Compare PnL between tokens
# /pnlchart — Export a chart link for dashboard visualizations
# /pnlalerts — Opt-in for alpha-grade PnL alerts triggered by RL models
