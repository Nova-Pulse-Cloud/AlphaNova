# File: telegram/commands/commands.py
# 🚀 Nova Pulse ALPHA – Elite async commands for Nova Pulse

import os
import logging
import time
import uuid
from aiogram import Router, types
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
wallet_manager = WalletManager(config)
trade_memory   = TradeMemory(config)
rl_tuner       = ReinforcementTuner(config)
adaptive_tp_sl = AdaptiveTPSL(memory=trade_memory, rl_agent=None)
engine         = CombinedSignalEngine(trade_memory, rl_tuner, adaptive_tp_sl)

# Redis for rate-limiting and auto-mode
redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

# Prometheus metrics\COMMANDS_RECEIVED = Counter("commands_received_total", "Total commands received by the bot")
COMMANDS_EXECUTED = Counter("commands_executed_total", "Total commands executed by the bot")
COMMANDS_FAILED   = Counter("commands_failed_total",   "Total command failures")
COMMANDS_LATENCY  = Histogram("commands_latency_seconds", "Latency of command executions")

logger = logging.getLogger("telegram_commands")
router = Router()

# Admin-only decorator
ADMIN_CHAT_ID = int(os.getenv("TELEGRAM_CHAT_ID", config.TELEGRAM_CHAT_ID or "0"))
def admin_only(func):
    async def wrapper(message: Message, *args, **kwargs):
        if int(message.from_user.id) != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        return await func(message, *args, **kwargs)
    return wrapper

# Rate-limit check (optional)
async def rate_limit_check(user_id: str, command: str) -> bool:
    key = f"{user_id}:{command}:last"
    last = await redis.get(key)
    now = time.time()
    if last and now - float(last) < 5:
        return False
    await redis.set(key, now, ex=5)
    return True

# /start: show command keyboard
@router.message(Command("start"))
@admin_only
async def start_handler(message: Message):
    keyboard = [
        ["/wallet", "/pnl", "/logs"],
        ["/signalcard", "/pnlchart", "/stats"],
        ["/social", "/whales", "/alpha"],
        ["/explain", "/auto", "/status"],
        ["/mode", "/mirror", "/exportdata"],
        ["/help"],
    ]
    markup = types.ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
    await message.answer(
        "👋 Welcome to Nova Pulse! Choose a command:",
        reply_markup=markup
    )
    COMMANDS_EXECUTED.inc()

# /wallet: show wallet public key
@router.message(Command("wallet"))
@admin_only
async def wallet_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    pubkey = wallet_manager.get_pubkey()
    await fire_plugin_event("cmd_wallet", {"pubkey": pubkey})
    await message.answer(
        f"💼 *Wallet Public Key:*
`{pubkey}`",
        parse_mode="Markdown"
    )
    COMMANDS_EXECUTED.inc()

# /pnl: display PnL card
@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    img = await generate_pnl_card(message.from_user.id)
    await fire_plugin_event("cmd_pnl_card", {})
    await message.answer_photo(img, caption="📈 PnL Summary")
    COMMANDS_EXECUTED.inc()

# /logs: show last trade logs
@router.message(Command("logs"))
@admin_only
async def logs_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    logs = await get_trade_log(message.from_user.id)
    await fire_plugin_event("cmd_logs", {})
    await message.answer(
        f"📝 *Last Trades:*
{logs}",
        parse_mode="Markdown"
    )
    COMMANDS_EXECUTED.inc()

# /signalcard SYMBOL
@router.message(Command("signalcard"))
@admin_only
async def signalcard_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❗ Usage: /signalcard <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = parts[1].upper()
    img = await generate_signal_card({"symbol": symbol})
    await fire_plugin_event("cmd_signal_card", {"symbol": symbol})
    await message.answer_photo(img, caption=f"🎴 Signal Card: {symbol}")
    COMMANDS_EXECUTED.inc()

# /pnlchart: equity curve chart
@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    img = await generate_pnl_image(message.from_user.id)
    await fire_plugin_event("cmd_pnl_chart", {})
    if img:
        await message.answer_photo(img, caption="📊 Equity Curve")
    else:
        await message.answer("No trade history available.")
    COMMANDS_EXECUTED.inc()

# /stats SYMBOL
@router.message(Command("stats"))
@admin_only
async def stats_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❗ Usage: /stats <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = parts[1].upper()
    stats = await get_token_stats(symbol)
    await fire_plugin_event("cmd_stats", {"symbol": symbol})
    await message.answer(
        f"📊 *Stats for {symbol}:*
{stats}",
        parse_mode="Markdown"
    )
    COMMANDS_EXECUTED.inc()

# /social: trending tokens
@router.message(Command("social"))
@admin_only
async def social_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    signals = await get_top_signals(limit=10, source="twitter")
    msg = "\n".join([f"#{s['symbol']} (score={s['score']:.2f})" for s in signals])
    await fire_plugin_event("cmd_social", {"count": len(signals)})
    await message.answer(f"🌐 *Trending Tokens:*
{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# /whales: recent whale swaps
@router.message(Command("whales"))
@admin_only
async def whales_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    whales = await get_recent_whale_tokens()
    msg = "\n".join([f"🐋 {w['symbol']} {w['action']} ${w['value']:.2f}" for w in whales])
    await fire_plugin_event("cmd_whales", {"count": len(whales)})
    await message.answer(f"🐋 *Whale Swaps:*
{msg}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# /alpha SYMBOL
@router.message(Command("alpha"))
@admin_only
async def alpha_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❗ Usage: /alpha <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = parts[1].upper()
    result = await engine.generate_signal(symbol, user=message.from_user.id)
    score = result.get("score", 0.0)
    await message.answer(f"🚀 *Alpha Signal for {symbol}:* {score:.2f}")
    if 'entry' in result:
        card = await generate_signal_card(result)
        await message.answer_photo(card)
    COMMANDS_EXECUTED.inc()

# /explain SYMBOL
@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("❗ Usage: /explain <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = parts[1].upper()
    explainer = SignalExplainerAI()
    res = await explainer.explain({"symbol": symbol, "components": {}, "timestamp": str(uuid.uuid1())})
    narrative = getattr(res, 'narrative', res.get('narrative', 'No explanation available.'))
    await message.answer(f"🧠 *Insight for {symbol}:*
{narrative}", parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()

# /auto: toggle auto-trading
@router.message(Command("auto"))
@admin_only
async def auto_toggle_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    key = f"auto_mode:{message.from_user.id}"
    mode = await redis.get(key)
    new = '1' if mode != '1' else '0'
    await redis.set(key, new)
    status = "Enabled" if new=='1' else "Disabled"
    await fire_plugin_event("cmd_auto_toggle", {"mode": status})
    await message.answer(f"🤖 Auto-Trading {status}")
    COMMANDS_EXECUTED.inc()

# /status: show auto-mode status
@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    mode = await redis.get(f"auto_mode:{message.from_user.id}")
    status = "🟢 ON" if mode=='1' else "🔴 OFF"
    await message.answer(f"Auto-Trading: {status}")
    COMMANDS_EXECUTED.inc()

# /mode MODE
@router.message(Command("mode"))
@admin_only
async def mode_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts)<2 or parts[1].lower() not in ("degen","moderate","safe"):
        await message.answer("❗ Usage: /mode <degen|moderate|safe>")
        COMMANDS_FAILED.inc()
        return
    await redis.set(f"risk_mode:{message.from_user.id}", parts[1].lower())
    await message.answer(f"🎯 Mode set to {parts[1].title()}")
    COMMANDS_EXECUTED.inc()

# /mirror SYMBOL
@router.message(Command("mirror"))
@admin_only
async def mirror_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    parts = message.text.split()
    if len(parts)<2:
        await message.answer("❗ Usage: /mirror <SYMBOL>")
        COMMANDS_FAILED.inc()
        return
    symbol = parts[1].upper()
    await fire_plugin_event("cmd_mirror", {"symbol": symbol})
    await message.answer(f"👥 Mirror trade: {symbol}")
    COMMANDS_EXECUTED.inc()

# /exportdata
@router.message(Command("exportdata"))
@admin_only
async def export_data_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    await fire_plugin_event("cmd_export_data", {})
    await message.answer("🗃️ Export coming soon.")
    COMMANDS_EXECUTED.inc()

# /help
@router.message(Command("help"))
@admin_only
async def help_handler(message: Message):
    COMMANDS_RECEIVED.inc()
    help_text = (
        "🤖 *Nova Pulse Commands:*\n"
        "/start — Show command menu\n"
        "/wallet — Wallet public key snapshot\n"
        "/pnl — Account PnL card\n"
        "/pnlchart — Equity curve plot\n"
        "/logs — Last trade logs\n"
        "/signalcard <SYMBOL> — Visual signal card\n"
        "/stats <SYMBOL> — Token memory stats\n"
        "/social — Trending tokens\n"
        "/whales — Whale swaps\n"
        "/alpha <SYMBOL> — Generate alpha signal\n"
        "/explain <SYMBOL> — AI explanation\n"
        "/auto — Toggle auto-trading\n"
        "/status — Show auto-trading status\n"
        "/mode <degen|moderate|safe> — Set risk mode\n"
        "/mirror <SYMBOL> — Mirror trades\n"
        "/exportdata — Export fills as CSV\n"
        "/help — This help menu"
    )
    await message.answer(help_text, parse_mode="Markdown")
    COMMANDS_EXECUTED.inc()
