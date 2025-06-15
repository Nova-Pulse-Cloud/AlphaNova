# File: telegram/commands/commands.py
# 🚀 Nova Pulse ALPHA – Elite async commands for Nova Pulse

import os
import logging
import time
import uuid
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message
import aioredis
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.wallet_management.wallet_manager import WalletManager
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from core.signal_generation.combined_signal_engine import CombinedSignalEngine
from core.utils.trade_logger import get_trade_log
from core.signal_generation.signal_sources import get_top_signals
from core.alpha_memory_mesh import get_token_stats
from core.top_wallet_tracer import get_recent_whale_tokens
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.utils.pnl_card import generate_pnl_card
from core.utils.pnl_chart import generate_pnl_image
from core.utils.signal_card import generate_signal_card
from core.utils.plugin_manager import fire_plugin_event
from core.signal_generation.reinforcement_learning import RLTrader

# Initialize dependencies
wallet_manager = WalletManager()
trade_memory = TradeMemory()
rl_tuner = ReinforcementTuner()
adaptive_tp_sl = AdaptiveTPSL(memory=trade_memory, rl_agent=None)
engine = CombinedSignalEngine(trade_memory, rl_tuner, adaptive_tp_sl)

redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

COMMANDS_RECEIVED = Counter("commands_received_total", "Total commands received by the bot")
COMMANDS_EXECUTED = Counter("commands_executed_total", "Total commands executed by the bot")
COMMANDS_FAILED = Counter("commands_failed_total", "Total command failures")
COMMANDS_LATENCY = Histogram("commands_latency_seconds", "Latency of command executions")

logger = logging.getLogger("telegram_commands")
router = Router()
ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", config.TELEGRAM_CHAT_ID or "7545342838"))

def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if int(message.from_user.id) != int(ADMIN_CHAT_ID):
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper

async def rate_limit_check(user_id: str, command: str):
    last_used = await redis.get(f"{user_id}:{command}:last_used")
    if last_used and time.time() - float(last_used) < 30:
        return False
    await redis.set(f"{user_id}:{command}:last_used", time.time(), ex=30)
    return True

@router.message(Command("wallet"))
@admin_only
async def wallet_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    snapshot = await wallet_manager.get_wallet_snapshot(user_id)
    await fire_plugin_event("cmd_wallet", {"user_id": user_id})
    await message.answer(f"💼 *Wallet Snapshot:*\n{snapshot}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    img = await generate_pnl_card(user_id)
    await fire_plugin_event("cmd_pnl_card", {"user_id": user_id})
    await message.answer_photo(img, caption="📈 PnL Summary")
    COMMANDS_EXECUTED.inc()

@router.message(Command("logs"))
@admin_only
async def logs_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    logs = await get_trade_log(user_id)
    await fire_plugin_event("cmd_logs", {"user_id": user_id})
    await message.answer(f"📝 *Last 20 Trades:*\n{logs}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

@router.message(Command("signalcard"))
@admin_only
async def signalcard_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /signalcard <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    img = await generate_signal_card({"symbol": symbol})
    await fire_plugin_event("cmd_signal_card", {"symbol": symbol})
    await message.answer_photo(img, caption=f"🎴 Signal Card for {symbol}")
    COMMANDS_EXECUTED.inc()

@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    img = await generate_pnl_image(user_id)
    await fire_plugin_event("cmd_pnl_chart", {"user_id": user_id})
    if img:
        await message.answer_photo(img, caption="📊 Equity Curve")
    else:
        await message.answer("No trade history available for chart.")
    COMMANDS_EXECUTED.inc()

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
    await message.answer(f"📊 *Memory Stats for {symbol}:*\n{stats}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

@router.message(Command("social"))
@admin_only
async def social_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    signals = await get_top_signals(limit=10, source="twitter")
    msg = "\n".join([f"#{s['symbol']} (score={s['score']:.2f})" for s in signals])
    await fire_plugin_event("cmd_social", {"count": len(signals)})
    await message.answer(f"🌐 *Trending Tokens on X:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

@router.message(Command("whales"))
@admin_only
async def whales_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    whales = await get_recent_whale_tokens()
    msg = "\n".join([f"🐋 {w['symbol']} {w['action']} ${w['value']:.2f}" for w in whales])
    await fire_plugin_event("cmd_whales", {"count": len(whales)})
    await message.answer(f"🐋 *Latest Whale Token Swaps:*\n{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

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
    if "composite" in result and "signal_strength" in result["composite"]:
        s_strength = result["composite"]["signal_strength"]
        await message.answer(
            f"🚀 *Alpha Signal for {symbol}:* {s_strength:.2f}",
            parse_mode="Markdown"
        )
        card_img = await generate_signal_card(result["composite"])
        await message.answer_photo(card_img, caption=f"🎴 Nova Pulse AI Card for {symbol}")
    else:
        await message.answer(f"⚠️ No alpha signal available for {symbol}")
    COMMANDS_EXECUTED.inc()

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
    result = await explainer.explain({"symbol": symbol, "components": {}, "timestamp": str(uuid.uuid1())})
    await fire_plugin_event("cmd_explain", {"symbol": symbol})
    narrative = result.narrative if hasattr(result, "narrative") else result.get("narrative", "")
    await message.answer(f"🧠 *{symbol} AI Insight:*\n\n{narrative}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

@router.message(Command("auto"))
@admin_only
async def auto_toggle_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = str(message.from_user.id)
    auto_key = f"auto_mode:{user_id}"
    mode = await redis.get(auto_key)
    if mode == "1":
        await redis.set(auto_key, "0")
        await fire_plugin_event("cmd_auto_toggle", {"user_id": user_id, "mode": "off"})
        await message.answer("❌ Auto-Trading Disabled.")
    else:
        await redis.set(auto_key, "1")
        await fire_plugin_event("cmd_auto_toggle", {"user_id": user_id, "mode": "on"})
        await message.answer("🚀 Auto-Trading Enabled.")
    COMMANDS_EXECUTED.inc()

@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = str(message.from_user.id)
    mode = await redis.get(f"auto_mode:{user_id}")
    status = "🟢 ON" if mode == "1" else "🔴 OFF"
    await fire_plugin_event("cmd_status", {"user_id": user_id, "status": status})
    await message.answer(f"🤖 Auto-Trading Status: {status}")
    COMMANDS_EXECUTED.inc()

@router.message(Command("mode"))
@admin_only
async def mode_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    user_id = str(message.from_user.id)
    if len(args) < 2 or args[1].lower() not in ("degen", "moderate", "safe"):
        await message.answer("❗ Usage: /mode <degen|moderate|safe>")
        COMMANDS_FAILED.inc()
        return
    selected = args[1].lower()
    await redis.set(f"risk_mode:{user_id}", selected)
    await fire_plugin_event("cmd_mode", {"user_id": user_id, "mode": selected})
    await message.answer(f"🎯 Trading mode set to: {selected.title()}")
    COMMANDS_EXECUTED.inc()

@router.message(Command("mirror"))
@admin_only
async def mirror_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    args = message.text.split()
    if len(args) < 2:
        await message.answer("❗ Usage: /mirror <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = args[1].upper()
    await fire_plugin_event("cmd_mirror", {"symbol": symbol, "user": message.from_user.id})
    await message.answer(f"👥 Mirror trade for {symbol} submitted!")
    COMMANDS_EXECUTED.inc()

@router.message(Command("exportdata"))
@admin_only
async def export_data_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    user_id = message.from_user.id
    await fire_plugin_event("cmd_export_data", {"user_id": user_id})
    await message.answer("🗃️ Export function is coming soon!")
    COMMANDS_EXECUTED.inc()

@router.message(Command("help"))
@admin_only
async def help_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    help_text = (
        "🤖 *Nova Pulse Commands:*\n"
        "/wallet — Wallet balance snapshot\n"
        "/pnl — Account PnL card\n"
        "/pnlchart — Equity curve plot\n"
        "/logs — Last 20 trade logs\n"
        "/signalcard <SYMBOL> — Visual signal card\n"
        "/stats <SYMBOL> — Token memory stats\n"
        "/social — Twitter trending tokens\n"
        "/whales — Whale swaps\n"
        "/alpha <SYMBOL> — Generate and view alpha signal (plus custom card)\n"
        "/explain <SYMBOL> — AI-powered explanation\n"
        "/auto — Toggle auto-trading\n"
        "/status — Auto mode status\n"
        "/mode <degen|moderate|safe> — Set trading risk mode\n"
        "/mirror <SYMBOL> — Mirror top wallet\n"
        "/exportdata — Export fills as CSV\n"
        "/help — Command list"
    )
    await message.answer(help_text, parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()
