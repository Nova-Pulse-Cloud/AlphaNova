# File: telegram/commands/streak_cmd.py
# Elite-tier aiogram streak command router—track, display, and share your win/loss streaks and cycle stats.
# Enhanced with inline UX and leaderboard-ready signals.
# ADMIN-ONLY: All commands are restricted to TELEGRAM_CHAT_ID

import os
import uuid
import logging
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton

from utils.pnl_tracker import get_streak_history, get_best_streak, get_cycle_stats
from utils.pnl_image_card import generate_streak_card
from utils.plugin_manager import fire_plugin_event
from core.signal_generation.reinforcement_learning import RLTrader  # Adding RL feedback functionality
from telegram.commands.commands import COMMANDS_EXECUTED, COMMANDS_FAILED

logger = logging.getLogger("telegram_streak_cmd")
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
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper


def streak_inline_buttons(user_id: int) -> InlineKeyboardMarkup:
    """
    Creates inline buttons for streak sharing and challenge modes.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔥 Share Streak", callback_data=f"share_streak:{user_id}")],
        [InlineKeyboardButton(text="👥 Challenge a Friend", callback_data="challenge_mode")]
    ])

@router.message(Command("streak"))
@admin_only
async def streak_handler(message: Message):
    """
    Fetches and displays the streak history of a user, and sends the streak card image.
    """
    user_id = message.from_user.id
    trace_id = f"streak-{uuid.uuid4().hex[:6]}"
    try:
        streaks = await get_streak_history(user=user_id)
        best = await get_best_streak(user=user_id)

        if streaks and best:
            summary = (
                f"🔥 *Your Current Streak:*\n"
                f"Wins: {streaks['wins']} | Losses: {streaks['losses']}\n"
                f"🏆 *Best:* {best['wins']}W/{best['losses']}L"
            )
            img = await generate_streak_card(user_id)
            await message.answer_photo(
                img,
                caption=summary,
                parse_mode="Markdown",
                reply_markup=streak_inline_buttons(user_id)
            )

            logger.info(f"[Streak] Sent streak card to user {user_id}")
            await fire_plugin_event(
                "cmd_streak",
                {"user_id": user_id, "streaks": streaks, "best": best, "trace_id": trace_id}
            )

            rl_trader = RLTrader()
            await rl_trader.reward(
                {"user_id": user_id}, {"streaks": streaks, "best": best}, user=user_id
            )
        else:
            await message.answer("📭 No streak data yet—start trading to build your streak!")
    except Exception as e:
        logger.error(f"[Streak] Error: {e}")
        await message.answer("⚠️ Failed to load streak data.")
        await fire_plugin_event(
            "cmd_streak_error",
            {"user_id": user_id, "error": str(e), "trace_id": trace_id}
        )

    COMMANDS_EXECUTED.inc()

@router.message(Command("cycle"))
@admin_only
async def cycle_handler(message: Message):
    """
    Fetches and displays the cycle stats of a user, including win rate and average PnL.
    """
    user_id = message.from_user.id
    trace_id = f"cycle-{uuid.uuid4().hex[:6]}"
    try:
        stats = await get_cycle_stats(user=user_id)

        if stats:
            win_rate = stats.get("win_rate", 0.0)
            avg_pnl = stats["pnl"] / stats["trades"] if stats["trades"] else 0
            msg = (
                f"🌀 *Your Cycle Stats:*\n"
                f"Cycle: {stats['cycle_name']}\n"
                f"Trades: {stats['trades']} | PnL: {stats['pnl']:.2f} | Avg: {avg_pnl:.2f}\n"
                f"Win Rate: {win_rate:.2%}"
            )
            await message.answer(msg, parse_mode="Markdown")

            logger.info(f"[Cycle] Sent cycle stats to user {user_id}")
            await fire_plugin_event(
                "cmd_cycle",
                {"user_id": user_id, "stats": stats, "trace_id": trace_id}
            )

            rl_trader = RLTrader()
            await rl_trader.reward(
                {"user_id": user_id}, {"cycle_stats": stats}, user=user_id
            )
        else:
            await message.answer(
                "🔄 No cycle stats yet. Trade during the next market cycle to track performance."
            )
    except Exception as e:
        logger.error(f"[Cycle] Error: {e}")
        await message.answer("⚠️ Failed to load cycle stats.")
        await fire_plugin_event(
            "cmd_cycle_error",
            {"user_id": user_id, "error": str(e), "trace_id": trace_id}
        )

    COMMANDS_EXECUTED.inc()

# === Future Plug-ins ===
# /streakcompare <token1> <token2> — Compare streak performance between tokens
# /streakhistory — Fetch a detailed history of streaks over time
