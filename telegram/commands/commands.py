# 🚀 Nova Pulse ALPHA – telegram/commands.py
# Async commands for Nova Pulse, integrated with RL, event-driven execution, and real-time updates

import os
import logging
import time
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
import aioredis
import uuid
from prometheus_client import Counter, Histogram

from core.wallet_manager import get_wallet_snapshot
from core.trade_logger import get_trade_log
from core.signal_generation.combined_signal_engine import CombinedSignalEngine
from core.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_explainer_ai import SignalExplainerAI
from utils.pnl_card import generate_pnl_card
from utils.pnl_chart import generate_pnl_image
from utils.signal_card import generate_signal_card
from utils.config import OPENAI_API_KEY, REDIS_URL
from core.plugin_manager import fire_plugin_event
from core.reinforcement_learning import RLTrader  # RL Feedback Integration

engine = CombinedSignalEngine(trade_memory, rl_tuner, adaptive_tp_sl)
result = await engine.generate_signal(symbol)

# Prometheus metrics
COMMANDS_RECEIVED = Counter("commands_received_total", "Total commands received by the bot")
COMMANDS_EXECUTED = Counter("commands_executed_total", "Total commands executed by the bot")
COMMANDS_FAILED = Counter("commands_failed_total", "Total command failures")
COMMANDS_LATENCY = Histogram("commands_latency_seconds", "Latency of command executions")

logger = logging.getLogger("telegram_commands")
router = Router()
redis = aioredis.from_url(REDIS_URL, decode_responses=True)

ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "7545342838"))

def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper

async def rate_limit_check(user_id: str, command: str):
    last_used = await redis.get(f"{user_id}:{command}:last_used")
    if last_used and time.time() - float(last_used) < 60:  # 1 minute cooldown
        await message.reply(f"Please wait a minute before trying {command} again.")
        return False
    await redis.set(f"{user_id}:{command}:last_used", time.time(), ex=60)
    return True

# === Wallet Command ===
@router.message(Command("wallet"))
@admin_only
async def wallet_handler(message: Message):
    user_id = message.from_user.id
    trace_id = f"wallet-{uuid.uuid4().hex[:6]}"
    snapshot = await get_wallet_snapshot(user_id)
    await fire_plugin_event("cmd_wallet", {"user_id": user_id, "trace_id": trace_id})
    await message.answer(f"\U0001F4BC *Wallet Snapshot:*\n{snapshot}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === PnL Command ===
@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    user_id = message.from_user.id
    trace_id = f"pnl-{uuid.uuid4().hex[:6]}"
    img = await generate_pnl_card(user_id)
    await fire_plugin_event("cmd_pnl_card", {"user_id": user_id, "trace_id": trace_id})
    await message.answer_photo(img, caption=f"\U0001F4C8 PnL Summary")
    COMMANDS_EXECUTED.inc()

# === Logs Command ===
@router.message(Command("logs"))
@admin_only
async def logs_handler(message: Message):
    user_id = message.from_user.id
    trace_id = f"logs-{uuid.uuid4().hex[:6]}"
    logs = await get_trade_log(user_id)
    await fire_plugin_event("cmd_logs", {"user_id": user_id, "trace_id": trace_id})
    await message.answer(f"\U0001F4DD *Last 20 Trades:*\n{logs}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Signal Card Command ===
@router.message(Command("signalcard"))
@admin_only
async def signalcard_handler(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /signalcard <mint>")
        COMMANDS_FAILED.inc()
        return
    mint = args[1].upper()
    trace_id = f"signalcard-{uuid.uuid4().hex[:6]}"
    img = await generate_signal_card(mint)
    await fire_plugin_event("cmd_signal_card", {"mint": mint, "trace_id": trace_id})
    await message.answer_photo(img, caption=f"\U0001F0CF Signal Card for {mint}")
    COMMANDS_EXECUTED.inc()

# === PnL Chart Command ===
@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    user_id = message.from_user.id
    trace_id = f"pnlchart-{uuid.uuid4().hex[:6]}"
    img = await generate_pnl_image(user_id)
    await fire_plugin_event("cmd_pnl_chart", {"user_id": user_id, "trace_id": trace_id})
    if img:
        await message.answer_photo(img, caption="\U0001F4C9 Equity Curve")
    else:
        await message.answer("No trade history available for chart.")
    COMMANDS_EXECUTED.inc()

# === Stats Command ===
@router.message(Command("stats"))
@admin_only
async def stats_handler(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /stats <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    trace_id = f"stats-{uuid.uuid4().hex[:6]}"
    stats = await get_token_stats(symbol)
    await fire_plugin_event("cmd_token_stats", {"symbol": symbol, "trace_id": trace_id})
    await message.answer(f"\U0001F4CA *Memory Stats for {symbol}:*\n{stats}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Social Command ===
@router.message(Command("social"))
@admin_only
async def social_handler(message: Message):
    signals = await get_top_signals(limit=10, source="twitter")
    msg = "\n".join([f"#{s['symbol']} (score={s['score']:.2f})" for s in signals])
    await fire_plugin_event("cmd_social", {"count": len(signals)})
    await message.answer(f"\U0001F310 *Trending Tokens on X:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Whales Command ===
@router.message(Command("whales"))
@admin_only
async def whales_handler(message: Message):
    whales = await get_recent_whale_tokens()
    msg = "\n".join([f"\U0001F40B {w['symbol']} {w['action']} ${w['value']:.2f}" for w in whales])
    await fire_plugin_event("cmd_whales", {"count": len(whales)})
    await message.answer(f"\U0001F40B *Latest Whale Token Swaps:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Alpha Command ===
@router.message(Command("alpha"))
@admin_only
async def alpha_handler(message: Message):
    signals = await get_top_signals(limit=5)
    msg = "\n".join([f"\U0001F680 {s['symbol']} — {s['score']:.2f}" for s in signals])
    await fire_plugin_event("cmd_alpha", {"signals": signals})
    await message.answer(f"\U0001F4E1 *Nova Alpha Engine Picks:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Explain Command ===
@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /explain <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    explainer = SignalExplainerAI()
    dummy_signal = {
        "symbol": symbol,
        "components": {
            "trend": 0.6,
            "sentiment": 0.5,
            "whales": 0.2,
            "liquidity": 0.4,
            "signal_strength": 0.77,
            "note": "Cycle volume anomaly"
        },
        "timestamp": str(uuid.uuid1())
    }
    result = await explainer.explain_signal(dummy_signal)
    await fire_plugin_event("cmd_explain", {"symbol": symbol})
    await message.answer(f"\U0001F9E0 *{symbol} AI Insight:*\n\n{result['narrative']}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Auto-Trading Mode ===
@router.message(Command("auto_on"))
@admin_only
async def auto_on_handler(message: Message):
    user_id = str(message.from_user.id)
    trace_id = f"auto_on-{uuid.uuid4().hex[:6]}"
    await redis.set(f"auto_mode:{user_id}", "1")
    await fire_plugin_event("cmd_auto_on", {"user_id": user_id, "trace_id": trace_id})
    await message.answer("\U0001F680 Auto-Trading Enabled.")
    COMMANDS_EXECUTED.inc()

@router.message(Command("auto_off"))
@admin_only
async def auto_off_handler(message: Message):
    user_id = str(message.from_user.id)
    trace_id = f"auto_off-{uuid.uuid4().hex[:6]}"
    await redis.delete(f"auto_mode:{user_id}")
    await fire_plugin_event("cmd_auto_off", {"user_id": user_id, "trace_id": trace_id})
    await message.answer("❌ Auto-Trading Disabled.")
    COMMANDS_EXECUTED.inc()

@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    user_id = str(message.from_user.id)
    trace_id = f"status-{uuid.uuid4().hex[:6]}"
    mode = await redis.get(f"auto_mode:{user_id}")
    status = "\U0001F7E2 ON" if mode == "1" else "\U0001F534 OFF"
    await fire_plugin_event("cmd_status", {"user_id": user_id, "trace_id": trace_id, "status": status})
    await message.answer(f"\U0001F916 Auto-Trading Status: {status}")
    COMMANDS_EXECUTED.inc()

@router.message(Command("help"))
@admin_only
async def help_handler(message: Message):
    await message.answer(
        "\U0001F916 *Nova Pulse Commands:*\n"
        "/wallet — Wallet balance snapshot\n"
        "/pnl — Account PnL card\n"
        "/pnlchart — Equity curve plot\n"
        "/logs — Last 20 trade logs\n"
        "/signalcard <mint> — Visual signal card\n"
        "/stats <SYMBOL> — Token memory stats\n"
        "/social — Twitter trending tokens\n"
        "/whales — Whale swaps\n"
        "/alpha — Alpha Engine ranked picks\n"
        "/explain <SYMBOL> — AI-powered explanation\n"
        "/auto_on — Turn on auto-trading mode\n"
        "/auto_off — Stop auto-trading mode\n"
        "/status — Auto mode status\n"
        "/help — Command list",
        parse_mode="Markdown"
    )
    COMMANDS_EXECUTED.inc()
