# 🚀 Nova Pulse ALPHA – core/execution/execute_trade.py
# The world's most advanced async, auditable, extensible crypto trade executor

import os
import asyncio
import logging
import hashlib
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Callable, Awaitable
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge
import httpx

from core.wallet_manager import WalletManager
from core.market_conditions import MarketConditionsService
from core.social_sentiment import score_token_sentiment
from core.whale_detector import get_recent_whale_activity
from core.reinforcement_learning import RLTrader
from core.alpha_memory_mesh import update_memory
from core.plugin_manager import PluginManager
from core.orderbook_overlay import get_orderbook_imbalance  # Import OrderBookOverlay
from openai import AsyncOpenAI

# Metrics
TRADES_EXECUTED = Counter("trades_executed_total", "Total trades executed")
TRADE_ERRORS = Counter("trade_errors_total", "Trade execution errors")
TRADE_LATENCY = Histogram("trade_latency_seconds", "Trade execution latency")
TRADE_PNL = Gauge("trade_pnl", "Realized PnL from trades")
TRADE_DLQ = Counter("trade_dlq_total", "Failed trades in DLQ")

# Event hooks for dashboard, RL mesh, ops, alerts
trade_event_hooks = []
def register_trade_event_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    trade_event_hooks.append(hook_fn)
async def fire_trade_event(event_name: str, data: Dict[str, Any]):
    for hook in trade_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logging.error(f"[Nova][Trade Event] Hook failure [{hook.__name__}]: {e}")

# DLQ for failed trades (consider using persistent DB/queue in prod)
FAILED_TRADES_DQL = []

logger = logging.getLogger("nova.execute_trade")
logger.setLevel(logging.INFO)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
JUPITER_API = os.getenv("JUPITER_API_URL", "https://quote-api.jup.ag/v6/quote")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

openai_client = AsyncOpenAI(api_key=OPENAI_API_KEY)

# Router registry: extend with any DEX, CEX, aggregator, or simulated exchange.
router_plugins = {}
def register_router(name: str, fn: Any):
    router_plugins[name] = fn

# RL-based position sizing
rl_trader = RLTrader()

# Utility: reproducibility hash
def trade_hash(trade: dict) -> str:
    m = hashlib.sha256()
    m.update(str(trade).encode())
    return m.hexdigest()

class TradeRequest(BaseModel):
    """
    Canonical trade request for Nova executor.
    """
    symbol: str
    side: str
    user_id: Optional[str] = "global"
    amount: float = 0.0
    slippage: float = 0.003
    dex: str = "jupiter"
    meta: Dict[str, Any] = {}
    trace_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))

class TradeResult(BaseModel):
    """
    Result of a trade execution, including audit and rationale.
    """
    tx_hash: Optional[str] = None
    success: bool = False
    pnl: Optional[float] = None
    error: Optional[str] = None
    audit_hash: Optional[str] = None
    rationale: Optional[str] = None
    timestamp: int
    trace_id: Optional[str] = None

async def retry_async(fn: Callable[[], Awaitable[Any]], retries: int = 3, delay: int = 2, err_msg: Optional[str] = None) -> Any:
    """
    Async retry wrapper for error-prone IO operations.
    """
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"{err_msg or 'Retry failed'}: {e}")
                raise
            await asyncio.sleep(delay * (2 ** attempt))

async def telegram_alert(msg: str) -> None:
    """
    Fire Telegram alert (ops, admin, live trading).
    """
    try:
        async with httpx.AsyncClient() as client:
            url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg}
            await client.post(url, json=payload)
    except Exception as e:
        logger.error(f"[Nova][Telegram Alert] Failed: {e}")

async def execute_trade(
    trade: TradeRequest,
    wallet_manager: WalletManager,
    market_service: MarketConditionsService,
    plugin_manager: PluginManager
) -> TradeResult:
    """
    The main async entrypoint for executing a single trade (real, no mocks).
    Live RL sizing, live DEX router, LLM rationale, event hooks, and full ops/metrics.
    """
    TRADES_EXECUTED.inc()
    start = datetime.utcnow()

    # RL/trust adaptive position sizing
    position_size = rl_trader.get_position_size(trade.symbol, trade.user_id)
    trade.amount = position_size

    # Wallet readiness check
    if not await wallet_manager.is_ready(trade.user_id):
        return TradeResult(success=False, error="Wallet not ready", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

    router_fn = router_plugins.get(trade.dex)
    if not router_fn:
        TRADE_ERRORS.inc()
        await fire_trade_event("router_not_found", trade.dict())
        return TradeResult(success=False, error=f"Router {trade.dex} not found", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

    try:
        # Fetch order book imbalance data to dynamically adjust TP/SL
        orderbook_imbalance = await get_orderbook_imbalance(trade.symbol)
        trade.slippage = max(trade.slippage, orderbook_imbalance * 0.01)  # Adjust slippage based on market depth

        async with TRADE_LATENCY.time():
            quote = await retry_async(lambda: router_fn.get_quote(trade), retries=2, delay=2, err_msg="DEX quote failed")
            tx_hash, pnl = await retry_async(lambda: router_fn.execute_swap(trade, quote), retries=2, delay=3, err_msg="Swap failed")
            rationale = await retry_async(lambda: explain_trade_llm(trade, quote, pnl), retries=2, delay=3, err_msg="LLM rationale failed")
            await update_memory(trade.symbol, "last_trade_pnl", pnl, user=trade.user_id)
            TRADE_PNL.set(pnl or 0.0)
            audit = trade.dict(); audit["tx_hash"] = tx_hash; audit["pnl"] = pnl
            h = trade_hash(audit)
            rl_trader.reward(trade.meta, {"pnl": pnl}, user=trade.user_id)
            result = TradeResult(tx_hash=tx_hash, success=True, pnl=pnl, rationale=rationale, audit_hash=h, timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)
            await update_memory(trade.symbol, "trade_audit", result.dict(), user=trade.user_id)
            await plugin_manager.post_trade(trade.dict(), result.dict())
            await fire_trade_event("trade_success", {**trade.dict(), **result.dict()})
            await telegram_alert(f"✅ Trade success: {trade.symbol} {trade.side} {position_size} PnL: {pnl}")
            return result
    except Exception as e:
        TRADE_ERRORS.inc()
        TRADE_DLQ.inc()
        err_msg = f"Trade failed: {e}"
        logger.error(f"[Nova][Trade Execution] {err_msg}")
        FAILED_TRADES_DQL.append({"trade": trade.dict(), "error": str(e), "trace_id": trade.trace_id})
        await plugin_manager.post_trade(trade.dict(), {"success": False, "error": str(e), "trace_id": trade.trace_id})
        await fire_trade_event("trade_failed", {"trade": trade.dict(), "error": str(e)})
        await telegram_alert(f"❌ Trade failed: {trade.symbol} {trade.side} reason: {e}")
        return TradeResult(success=False, error=err_msg, timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

async def explain_trade_llm(trade: TradeRequest, quote: dict, pnl: float) -> str:
    """
    Generate a LLM rationale for the trade (for PNL cards, dashboards, or Telegram).
    """
    prompt = (
        f"Explain the rationale for executing a {trade.side} trade on {trade.symbol} using venue {trade.dex} "
        f"for amount {trade.amount}. Quote details: {quote}. Realized PnL: {pnl}. "
        f"Respond as an expert trading AI."
    )
    try:
        result = await openai_client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[{"role": "user", "content": prompt}]
        )
        return result.choices[0].message.content
    except Exception as e:
        logger.warning(f"[Nova][LLM Rationale Fallback]: {e}")
        return f"Trade rationale: {trade.symbol}, {trade.side}."

# Example Jupiter router plugin (live-ready)
def jupiter_router_plugin():
    class JupiterRouter:
        @staticmethod
        async def get_quote(trade: TradeRequest):
            params = {
                "inputMint": trade.meta.get("input_mint"),
                "outputMint": trade.meta.get("output_mint"),
                "amount": trade.amount,
                "slippageBps": int(trade.slippage * 10000)
            }
            async with httpx.AsyncClient() as client:
                resp = await client.get(JUPITER_API, params=params)
                return resp.json()
        @staticmethod
        async def execute_swap(trade: TradeRequest, quote: dict):
            # The WalletManager must implement this method.
            return await wallet_manager.submit_swap(trade.symbol, quote, trade.amount, trade.user_id)
    return JupiterRouter

register_router("jupiter", jupiter_router_plugin())
