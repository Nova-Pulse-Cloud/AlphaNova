# File: core/execution/execute_trade.py
# 🚀 Nova Pulse ALPHA – Ultra‑Grade Trade Executor (Phantom / Jupiter)
"""
Executes trades with RL‑driven sizing, on‑chain routing, Prometheus metrics,
OpenAI rationale, and full DLQ / audit hashing — worthy of the #1 crypto bot.
"""

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, Optional

import httpx
from openai import AsyncOpenAI
from pydantic import BaseModel, Field
from prometheus_client import Counter, Gauge, Histogram

from utils.config import Config
from core.wallet_management.wallet_manager import WalletManager
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from core.signal_generation.reinforcement_learning import RLTrader
from core.alpha_memory_mesh import update_memory
from utils.plugin_manager import PluginManager

# ────────────────────────────────────────────────────────────────────────────────
# Init & Globals
# ────────────────────────────────────────────────────────────────────────────────
config = Config()
openai_client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
logger = logging.getLogger("nova.execute_trade")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TRADES_EXECUTED = Counter("trades_executed_total", "Total trades executed")
TRADE_ERRORS    = Counter("trade_errors_total", "Trade execution errors")
TRADE_LATENCY   = Histogram("trade_latency_seconds", "Trade execution latency")
TRADE_PNL       = Gauge("trade_pnl", "Realized PnL from trades")
TRADE_DLQ       = Counter("trade_dlq_total", "Failed trades in DLQ")

# ────────────────────────────────────────────────────────────────────────────────
# Event Hooks & DLQ
# ────────────────────────────────────────────────────────────────────────────────
trade_event_hooks: list[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []
FAILED_TRADES_DLQ: list = []

def register_trade_event_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    trade_event_hooks.append(hook_fn)

async def _fire_trade_event(event_name: str, data: Dict[str, Any]):
    for hook in trade_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[Trade Event] Hook failure [{hook.__name__}]: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Pydantic Models
# ────────────────────────────────────────────────────────────────────────────────
class TradeRequest(BaseModel):
    symbol: str
    side: str  # "buy" or "sell"
    user_id: Optional[str] = "global"
    amount: float = 0.0
    slippage: float = 0.003
    dex: str = "jupiter"
    meta: Dict[str, Any] = {}
    trace_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))

class TradeResult(BaseModel):
    tx_hash: Optional[str] = None
    success: bool = False
    pnl: Optional[float] = None
    error: Optional[str] = None
    audit_hash: Optional[str] = None
    rationale: Optional[str] = None
    timestamp: int
    trace_id: Optional[str] = None

# ────────────────────────────────────────────────────────────────────────────────
# Helper Functions
# ────────────────────────────────────────────────────────────────────────────────
async def _retry_async(fn: Callable[[], Awaitable[Any]], retries: int, delay: int, err_msg: str):
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"{err_msg}: {e}")
                raise
            await asyncio.sleep(delay * (2 ** attempt))

async def _telegram_alert(msg: str):
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage",
                json={"chat_id": config.TELEGRAM_CHAT_ID, "text": msg},
                timeout=10,
            )
    except Exception as e:
        logger.error(f"[Telegram Alert] {e}")

async def _explain_trade_llm(trade: TradeRequest, quote: dict, pnl: float) -> str:
    prompt = (
        f"Explain the rationale for a {trade.side.upper()} trade on {trade.symbol} via {trade.dex}. "
        f"Trade size: {trade.amount}. Quote details: {quote}. Realized PnL: {pnl:.4f}. "
        "Respond concisely as an expert trading AI with emojis."
    )
    try:
        res = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}],
            timeout=15,
        )
        return res.choices[0].message.content.strip()
    except Exception as e:
        logger.warning(f"[LLM Rationale Fallback] {e}")
        return f"Trade rationale for {trade.symbol} ({trade.side})."

# ────────────────────────────────────────────────────────────────────────────────
# Core: execute_trade
# ────────────────────────────────────────────────────────────────────────────────
async def execute_trade(
    trade: TradeRequest,
    *,
    wallet_manager: WalletManager,
    market_service: MarketConditionsService,
    plugin_manager: PluginManager,
) -> TradeResult:
    """Execute a single live trade with full audit trail."""
    TRADES_EXECUTED.inc()
    rl_trader = RLTrader()
    # RL sizing
    trade.amount = rl_trader.get_position_size(trade.symbol, trade.user_id)
    # Wallet readiness
    if not await wallet_manager.is_ready(trade.user_id):
        return TradeResult(success=False, error="Wallet not ready", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)
    router_fn = plugin_manager.get_router(trade.dex)
    if not router_fn:
        TRADE_ERRORS.inc()
        await _fire_trade_event("router_not_found", trade.dict())
        return TradeResult(success=False, error=f"Router {trade.dex} not found", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)
    try:
        # Market‑aware slippage
        ob_imbalance = await get_orderbook_imbalance(trade.symbol)
        trade.slippage = max(trade.slippage, ob_imbalance * 0.01)
        async with TRADE_LATENCY.time():
            quote   = await _retry_async(lambda: router_fn.get_quote(trade), 2, 2, "DEX quote failed")
            tx_hash, pnl = await _retry_async(lambda: router_fn.execute_swap(trade, quote), 2, 3, "Swap failed")
            rationale    = await _retry_async(lambda: _explain_trade_llm(trade, quote, pnl), 2, 3, "LLM rationale failed")
        # Audit & Memory
        audit_data  = {**trade.dict(), "tx_hash": tx_hash, "pnl": pnl}
        audit_hash  = hashlib.sha256(str(audit_data).encode()).hexdigest()
        await update_memory(trade.symbol, "trade_audit", audit_data, user=trade.user_id)
        rl_trader.reward(trade.meta, {"pnl": pnl}, user=trade.user_id)
        TRADE_PNL.set(pnl or 0.0)
        result = TradeResult(
            tx_hash=tx_hash,
            success=True,
            pnl=pnl,
            audit_hash=audit_hash,
            rationale=rationale,
            timestamp=int(datetime.utcnow().timestamp()),
            trace_id=trade.trace_id,
        )
        await plugin_manager.post_trade(trade.dict(), result.dict())
        await _fire_trade_event("trade_success", {**trade.dict(), **result.dict()})
        await _telegram_alert(f"✅ Trade success: {trade.symbol} {trade.side} PnL: {pnl:.4f}")
        return result
    except Exception as e:
        TRADE_ERRORS.inc()
        TRADE_DLQ.inc()
        err = str(e)
        logger.error(f"[Trade Execution] {err}")
        FAILED_TRADES_DLQ.append({"trade": trade.dict(), "error": err, "trace_id": trade.trace_id})
        await plugin_manager.post_trade(trade.dict(), {"success": False, "error": err, "trace_id": trade.trace_id})
        await _fire_trade_event("trade_failed", {"trade": trade.dict(), "error": err})
        await _telegram_alert(f"❌ Trade failed: {trade.symbol} {trade.side} reason: {err}")
        return TradeResult(success=False, error=err, timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)
