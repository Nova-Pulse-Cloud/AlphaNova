# File: src/telegram/commands/commands.py
# 🚀 Nova Pulse ALPHA – Elite async commands for Nova Pulse

import os
import time
import uuid
import logging
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
from core.data_collection.social_sentiment import score_token_sentiment  # fixed import

# Initialize dependencies
wallet_manager = WalletManager(config)
trade_memory   = TradeMemory(config)
rl_tuner       = ReinforcementTuner()
adaptive_tp_sl = AdaptiveTPSL(memory=trade_memory, rl_agent=None)
engine         = CombinedSignalEngine(trade_memory, rl_tuner, adaptive_tp_sl)

# Redis for rate-limiting and auto-mode
auto_redis = aioredis.from_url(config.REDIS_URL, decode_responses=True)

# Prometheus metrics
COMMANDS_RECEIVED = Counter("commands_received_total", "Total commands received by the bot")
COMMANDS_EXECUTED = Counter("commands_executed_total", "Total commands executed by the bot")
COMMANDS_FAILED   = Counter("commands_failed_total",   "Total command failures")
COMMANDS_LATENCY  = Histogram("commands_latency_seconds", "Latency of command executions")

logger = logging.getLogger("telegram_commands")
router = Router()

# Admin-only decorator
ADMIN_CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID", config.TELEGRAM_CHAT_ID or "0"))

def admin_only(handler):
    async def wrapper(message: Message, *args, **kwargs):
        COMMANDS_RECEIVED.inc()
        if str(message.from_user.id) != ADMIN_CHAT_ID:
            await message.reply("❌ Access denied. This bot is private.")
            COMMANDS_FAILED.inc()
            return
        start = time.time()
        result = await handler(message, *args, **kwargs)
        COMMANDS_EXECUTED.inc()
        COMMANDS_LATENCY.observe(time.time() - start)
        return result
    return wrapper

# Rate-limit (optional)
async def rate_limit_check(user_id: str, command: str) -> bool:
    key = f"{user_id}:{command}:last"
    last = await auto_redis.get(key)
    now = time.time()
    if last and now - float(last) < 2:
        return False
    await auto_redis.set(key, now, ex=2)
    return True

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

@router.message(Command("help"))
@admin_only
async def help_handler(message: Message):
    help_text = (
        "🤖 *Nova Pulse Commands:*\n"
        "/start — Show command menu\n"
        "/wallet — Wallet public key snapshot\n"
        "/pnl — Account PnL card\n"
        "/pnlchart — Equity curve plot\n"
        "/logs — Last trade logs\n"
        "/signalcard <SYMBOL> — Visual signal card\n"
        "/stats <SYMBOL> — Token memory stats\n"
        "/social <SYMBOL> — Social sentiment\n"
        "/whales — Whale swaps summary\n"
        "/alpha <SYMBOL> — Generate alpha signal\n"
        "/explain <SYMBOL> — AI explanation\n"
        "/auto on|off — Toggle auto trading\n"
        "/status — Show auto trading status\n"
        "/mode <degen|moderate|safe> — Set risk mode\n"
        "/mirror <SYMBOL> — Mirror trade via plugin\n"
        "/exportdata — Export fills as CSV\n"
    )
    await message.answer(help_text, parse_mode="Markdown")

@router.message(Command("wallet"))
@admin_only
async def wallet_handler(message: Message):
    pubkey = wallet_manager.get_pubkey()
    await fire_plugin_event("cmd_wallet", {"pubkey": pubkey})
    await message.answer(f"💼 *Wallet Public Key:*`{pubkey}`", parse_mode="Markdown")

@router.message(Command("pnl"))
@admin_only
async def pnl_handler(message: Message):
    img = await generate_pnl_card(message.from_user.id)
    await fire_plugin_event("cmd_pnl_card", {})
    await message.answer_photo(img, caption="📈 PnL Summary")

@router.message(Command("logs"))
@admin_only
async def logs_handler(message: Message):
    logs = await get_trade_log(message.from_user.id)
    await fire_plugin_event("cmd_logs", {})
    await message.answer(f"📝 *Last Trades:*\n{logs}", parse_mode="Markdown")

@router.message(Command("signalcard"))
@admin_only
async def signalcard_handler(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /signalcard <SYMBOL>")
        return
    symbol = parts[1].upper()
    img = await generate_signal_card({"symbol": symbol})
    await fire_plugin_event("cmd_signal_card", {"symbol": symbol})
    await message.answer_photo(img, caption=f"🎴 Signal Card: {symbol}")

@router.message(Command("pnlchart"))
@admin_only
async def pnlchart_handler(message: Message):
    img = await generate_pnl_image(message.from_user.id)
    if img:
        await message.answer_photo(img, caption="📊 Equity Curve")
    else:
        await message.answer("No trade history available.")

@router.message(Command("stats"))
@admin_only
async def stats_handler(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /stats <SYMBOL>")
        return
    symbol = parts[1].upper()
    stats = await get_token_stats(symbol)
    await fire_plugin_event("cmd_stats", {"symbol": symbol})
    await message.answer(f"📊 *Stats for {symbol}:*\n{stats}", parse_mode="Markdown")

@router.message(Command("social"))
@admin_only
async def social_handler(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /social <SYMBOL>")
        return
    symbol = parts[1].upper()
    sentiment = await score_token_sentiment(symbol)
    score = sentiment.get("sentiment_score", 0.0)
    await fire_plugin_event("cmd_social", {"symbol": symbol, "score": score})
    await message.answer(f"🌐 *Social {symbol}:* `{score:.4f}`", parse_mode="Markdown")

@router.message(Command("whales"))
@admin_only
async def whales_handler(message: Message):
    text = await get_recent_whale_tokens()
    await fire_plugin_event("cmd_whales", {})
    await message.answer(f"🐋 *Whale Swaps:*\n{text}", parse_mode="Markdown")

@router.message(Command("alpha"))
@admin_only
async def alpha_handler(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /alpha <SYMBOL>")
        return
    symbol = parts[1].upper()
    result = await engine.generate_signal(symbol, user=message.from_user.id)
    score = result.get("score", 0.0)
    await message.answer(f"🚀 *Alpha Signal for {symbol}:* `{score:.2f}`", parse_mode="Markdown")
    if result.get("entry") is not None:
        img = await generate_signal_card(result)
        await message.answer_photo(img)

@router.message(Command("explain"))
@admin_only
async def explain_handler(message: Message):
    parts = message.text.split()
    if len(parts) < 2:
        await message.answer("Usage: /explain <SYMBOL>")
        return
    symbol = parts[1].upper()
    explainer = SignalExplainerAI(config.OPENAI_API_KEY)
    res = await explainer.explain(symbol)
    narrative = getattr(res, 'narrative', res.get('narrative', 'No explanation.'))
    await message.answer(f"🧠 *Insight for {symbol}:*\n{narrative}", parse_mode="Markdown")

@router.message(Command("auto"))
@admin_only
async def auto_handler(message: Message):
    parts = message.text.lower().split()
    if len(parts) != 2 or parts[1] not in ("on", "off"):
        await message.answer("Usage: /auto on|off")
        return
    new = '1' if parts[1]=='on' else '0'
    await auto_redis.set(f"auto:{message.from_user.id}", new)
    await fire_plugin_event("telegram_auto_toggle", {"mode": new, "user": message.from_user.id})
    await message.answer(f"🤖 Auto-Trading {'ENABLED' if new=='1' else 'DISABLED'}")

@router.message(Command("status"))
@admin_only
async def status_handler(message: Message):
    mode = await auto_redis.get(f"auto:{message.from_user.id}") or '0'
    status = "🟢 ON" if mode=='1' else "🔴 OFF"
    await message.answer(f"*Auto-Trading status:* {status}", parse_mode="Markdown")

@router.message(Command("mode"))
@admin_only
async def mode_handler(message: Message):
    parts = message.text.split()
    if len(parts)<2 or parts[1].lower() not in ("degen","moderate","safe"):
        await message.answer("Usage: /mode <degen|moderate|safe>")
        return
    await auto_redis.set(f"risk_mode:{message.from_user.id}", parts[1].lower())
    await message.answer(f"🎯 Mode set to {parts[1].title()}")

@router.message(Command("mirror"))
@admin_only
async def mirror_handler(message: Message):
    parts = message.text.split()
    if len(parts)<2:
        await message.answer("Usage: /mirror <SYMBOL>")
        return
    symbol = parts[1].upper()
    await fire_plugin_event("cmd_mirror", {"symbol": symbol, "user": message.from_user.id})
    await message.answer(f"👥 Mirror trade: {symbol}")

@router.message(Command("exportdata"))
@admin_only
async def export_handler(message: Message):
    await fire_plugin_event("cmd_export_data", {"user": message.from_user.id})
    await message.answer("🗃️ Export coming soon.")

# EOF
