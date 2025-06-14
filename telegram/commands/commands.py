# File: telegram/commands/commands.py
# 🚀 Nova Pulse ALPHA – telegram/commands.py
# Async commands for Nova Pulse, integrated with RL, event-driven execution, and real-time updates

import os
import logging
import time
import uuid
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
import aioredis
from prometheus_client import Counter, Histogram

from utils.config import Config
from core.wallet_management.wallet_manager import WalletManager
from utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from core.signal_generation.combined_signal_engine import CombinedSignalEngine
from core.trade_logger import get_trade_log
from core.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_explainer_ai import SignalExplainerAI
from utils.pnl_card import generate_pnl_card
from utils.pnl_chart import generate_pnl_image
from utils.signal_card import generate_signal_card
from utils.plugin_manager import fire_plugin_event
from core.reinforcement_learning import RLTrader  # RL Feedback Integration

# Initialize core dependencies
config = Config()
wallet_manager = WalletManager()
trade_memory = TradeMemory()
rl_tuner = ReinforcementTuner()
adaptive_tp_sl = AdaptiveTPSL(memory=trade_memory, rl_agent=None)
engine = CombinedSignalEngine(trade_memory, rl_tuner, adaptive_tp_sl)

# Redis for rate limiting and auto-mode storage
redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

# Prometheus metrics
COMMANDS_RECEIVED = Counter("commands_received_total", "Total commands received by the bot")
COMMANDS_EXECUTED = Counter("commands_executed_total", "Total commands executed by the bot")
COMMANDS_FAILED = Counter("commands_failed_total", "Total command failures")
COMMANDS_LATENCY = Histogram("commands_latency_seconds", "Latency of command executions")

logger = logging.getLogger("telegram_commands")
router = Router()

ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", "7545342838"))

# Decorator to restrict commands to admin
def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if message.from_user.id != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper

# Rate limiting helper (per-command cooldown)
async def rate_limit_check(user_id: str, command: str):
    last_used = await redis.get(f"{user_id}:{command}:last_used")
    if last_used and time.time() - float(last_used) < 60:
        return False
    await redis.set(f"{user_id}:{command}:last_used", time.time(), ex=60)
    return True

# === Wallet Command ===
@router.message(Command("wallet"))
@admin_only
async def wallet_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    snapshot = await wallet_manager.get_wallet_snapshot(user_id)
    await fire_plugin_event("cmd_wallet", {"user_id": user_id})
    await message.answer(f"\U0001F4BC *Wallet Snapshot:*\n{snapshot}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === PnL Command ===
@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    img = await generate_pnl_card(user_id)
    await fire_plugin_event("cmd_pnl_card", {"user_id": user_id})
    await message.answer_photo(img, caption="\U0001F4C8 PnL Summary")
    COMMANDS_EXECUTED.inc()

# === Logs Command ===
@router.message(Command("logs"))
@admin_only
async def logs_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    logs = await get_trade_log(user_id)
    await fire_plugin_event("cmd_logs", {"user_id": user_id})
    await message.answer(f"\U0001F4DD *Last 20 Trades:*\n{logs}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Signal Card Command ===
@router.message(Command("signalcard"))
@admin_only
async def signalcard_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /signalcard <mint>")
        COMMANDS_FAILED.inc()
        return
    mint = args[1].upper()
    img = await generate_signal_card(mint)
    await fire_plugin_event("cmd_signal_card", {"mint": mint})
    await message.answer_photo(img, caption=f"\U0001F0CF Signal Card for {mint}")
    COMMANDS_EXECUTED.inc()

# === PnL Chart Command ===
@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    img = await generate_pnl_image(user_id)
    await fire_plugin_event("cmd_pnl_chart", {"user_id": user_id})
    if img:
        await message.answer_photo(img, caption="\U0001F4C9 Equity Curve")
    else:
        await message.answer("No trade history available for chart.")
    COMMANDS_EXECUTED.inc()

# === Stats Command ===
@router.message(Command("stats"))
@admin_only
async def stats_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /stats <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    stats = await get_token_stats(symbol)
    await fire_plugin_event("cmd_token_stats", {"symbol": symbol})
    await message.answer(f"\U0001F4CA *Memory Stats for {symbol}:*\n{stats}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Social Command ===
@router.message(Command("social"))
@admin_only
async def social_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    signals = await get_top_signals(limit=10, source="twitter")
    msg = "\n".join([f"#{s['symbol']} (score={s['score']:.2f})" for s in signals])
    await fire_plugin_event("cmd_social", {"count": len(signals)})
    await message.answer(f"\U0001F310 *Trending Tokens on X:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Whales Command ===
@router.message(Command("whales"))
@admin_only
async def whales_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    whales = await get_recent_whale_tokens()
    msg = "\n".join([f"\U0001F40B {w['symbol']} {w['action']} ${w['value']:.2f}" for w in whales])
    await fire_plugin_event("cmd_whales", {"count": len(whales)})
    await message.answer(f"\U0001F40B *Latest Whale Token Swaps:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Alpha Command ===
@router.message(Command("alpha"))
@admin_only
async def alpha_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /alpha <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    result = await engine.generate_signal(symbol, user=message.from_user.id)
    await message.answer(
        f"🚀 *Alpha Signal for {symbol}:* {result['composite']['signal_strength']:.2f}",
        parse_mode="Markdown"
    )
    COMMANDS_EXECUTED.inc()

# === Explain Command ===
@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /explain <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    explainer = SignalExplainerAI()
    result = await explainer.explain_signal({"symbol": symbol, "components": {}, "timestamp": str(uuid.uuid1())})
    await fire_plugin_event("cmd_explain", {"symbol": symbol})
    await message.answer(f"\U0001F9E0 *{symbol} AI Insight:*\n\n{result.get('narrative', '')}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# === Auto-Trading Mode ===
@router.message(Command("auto_on"))
@admin_only
async def auto_on_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = str(message.from_user.id)
    await redis.set(f"auto_mode:{user_id}", "1")
    await fire_plugin_event("cmd_auto_on", {"user_id": user_id})
    await message.answer("\U0001F680 Auto-Trading Enabled.")
    COMMANDS_EXECUTED.inc()

@router.message(Command("auto_off"))
@admin_only
async def auto_off_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = str(message.from_user.id)
    await redis.delete(f"auto_mode:{user_id}")
    await fire_plugin_event("cmd_auto_off", {"user_id": user_id})
    await message.answer("❌ Auto-Trading Disabled.")
    COMMANDS_EXECUTED.inc()

# === Status Command ===
@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = str(message.from_user.id)
    mode = await redis.get(f"auto_mode:{user_id}")
    status = "\U0001F7E2 ON" if mode == "1" else "\U0001F534 OFF"
    await fire_plugin_event("cmd_status", {"user_id": user_id, "status": status})
    await message.answer(f"\U0001F916 Auto-Trading Status: {status}")
    COMMANDS_EXECUTED.inc()

# === Help Command ===
@router.message(Command("help"))
@admin_only
async def help_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    help_text = (
        "\U0001F916 *Nova Pulse Commands:*\n"
        "/wallet — Wallet balance snapshot\n"
        "/pnl — Account PnL card\n"
        "/pnlchart — Equity curve plot\n"
        "/logs — Last 20 trade logs\n"
        "/signalcard <mint> — Visual signal card\n"
        "/stats <SYMBOL> — Token memory stats\n"
        "/social — Twitter trending tokens\n"
        "/whales — Whale swaps\n"
        "/alpha <SYMBOL> — Generate and view alpha signal\n"
        "/explain <SYMBOL> — AI-powered explanation\n"
        "/auto_on — Turn on auto-trading mode\n"
        "/auto_off — Stop auto-trading mode\n"
        "/status — Auto mode status\n"
        "/help — Command list"
    )
    await message.answer(help_text, parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()
