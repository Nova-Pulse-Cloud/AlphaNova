# File: core/execution/execute_trade.py

import asyncio
import logging
import hashlib
import uuid
from datetime import datetime
from typing import Any, Dict, Optional, Callable, Awaitable

import httpx
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge
from openai import AsyncOpenAI

from utils.config import Config
from core.wallet_management.wallet_manager import WalletManager
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.social_sentiment import score_token_sentiment
from core.data_collection.whale_detector import get_recent_whale_activity
from core.signal_generation.reinforcement_learning import RLTrader
from core.alpha_memory_mesh import update_memory
from utils.plugin_manager import PluginManager
from core.data_collection.orderbook_overlay import get_orderbook_imbalance

# Initialize config and external clients
config = Config()
openai_client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)

# Metrics
TRADES_EXECUTED = Counter("trades_executed_total", "Total trades executed")
TRADE_ERRORS    = Counter("trade_errors_total", "Trade execution errors")
TRADE_LATENCY   = Histogram("trade_latency_seconds", "Trade execution latency")
TRADE_PNL       = Gauge("trade_pnl", "Realized PnL from trades")
TRADE_DLQ       = Counter("trade_dlq_total", "Failed trades in DLQ")

# Event hooks
trade_event_hooks: list[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def register_trade_event_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    trade_event_hooks.append(hook_fn)

async def fire_trade_event(event_name: str, data: Dict[str, Any]):
    for hook in trade_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logging.error(f"[Trade Event] Hook failure [{hook.__name__}]: {e}")

# DLQ
FAILED_TRADES_DLQ: list = []

logger = logging.getLogger("core.execute_trade")
logger.setLevel(logging.INFO)

class TradeRequest(BaseModel):
    symbol: str
    side: str
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

async def retry_async(fn: Callable[[], Awaitable[Any]], retries: int = 3, delay: int = 2, err_msg: Optional[str] = None) -> Any:
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"{err_msg or 'Retry failed'}: {e}")
                raise
            await asyncio.sleep(delay * (2 ** attempt))

async def telegram_alert(msg: str) -> None:
    try:
        async with httpx.AsyncClient() as client:
            url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
            payload = {"chat_id": config.TELEGRAM_CHAT_ID, "text": msg}
            await client.post(url, json=payload)
    except Exception as e:
        logger.error(f"[Telegram Alert] Failed: {e}")

async def execute_trade(
    trade: TradeRequest,
    wallet_manager: WalletManager,
    market_service: MarketConditionsService,
    plugin_manager: PluginManager
) -> TradeResult:
    TRADES_EXECUTED.inc()
    start = datetime.utcnow()

    # RL sizing
    rl = RLTrader()
    trade.amount = rl.get_position_size(trade.symbol, trade.user_id)

    # Wallet readiness
    if not await wallet_manager.is_ready(trade.user_id):
        return TradeResult(success=False, error="Wallet not ready", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

    router_fn = plugin_manager.get_router(trade.dex)
    if not router_fn:
        TRADE_ERRORS.inc()
        await fire_trade_event("router_not_found", trade.dict())
        return TradeResult(success=False, error=f"Router {trade.dex} not found", timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

    try:
        # Order book insight
        ob = await get_orderbook_imbalance(trade.symbol)
        trade.slippage = max(trade.slippage, ob * 0.01)

        async with TRADE_LATENCY.time():
            quote = await retry_async(lambda: router_fn.get_quote(trade), retries=2, delay=2, err_msg="DEX quote failed")
            tx_hash, pnl = await retry_async(lambda: router_fn.execute_swap(trade, quote), retries=2, delay=3, err_msg="Swap failed")
            rationale = await retry_async(lambda: explain_trade_llm(trade, quote, pnl), retries=2, delay=3, err_msg="LLM rationale failed")

            # Memory & metrics
            await update_memory(trade.symbol, "last_trade_pnl", pnl, user=trade.user_id)
            TRADE_PNL.set(pnl or 0.0)
            audit_data = {**trade.dict(), "tx_hash": tx_hash, "pnl": pnl}
            h = hashlib.sha256(str(audit_data).encode()).hexdigest()
            rl.reward(trade.meta, {"pnl": pnl}, user=trade.user_id)

            result = TradeResult(
                tx_hash=tx_hash,
                success=True,
                pnl=pnl,
                rationale=rationale,
                audit_hash=h,
                timestamp=int(datetime.utcnow().timestamp()),
                trace_id=trade.trace_id
            )
            await update_memory(trade.symbol, "trade_audit", result.dict(), user=trade.user_id)
            await plugin_manager.post_trade(trade.dict(), result.dict())
            await fire_trade_event("trade_success", {**trade.dict(), **result.dict()})
            await telegram_alert(f"✅ Trade success: {trade.symbol} {trade.side} PnL: {pnl}")
            return result
    except Exception as e:
        TRADE_ERRORS.inc()
        TRADE_DLQ.inc()
        err = str(e)
        logger.error(f"[Trade Execution] {err}")
        FAILED_TRADES_DLQ.append({"trade": trade.dict(), "error": err, "trace_id": trade.trace_id})
        await plugin_manager.post_trade(trade.dict(), {"success": False, "error": err, "trace_id": trade.trace_id})
        await fire_trade_event("trade_failed", {"trade": trade.dict(), "error": err})
        await telegram_alert(f"❌ Trade failed: {trade.symbol} {trade.side} reason: {err}")
        return TradeResult(success=False, error=err, timestamp=int(datetime.utcnow().timestamp()), trace_id=trade.trace_id)

async def explain_trade_llm(trade: TradeRequest, quote: dict, pnl: float) -> str:
    prompt = (
        f"Explain rationale for {trade.side} on {trade.symbol} 
