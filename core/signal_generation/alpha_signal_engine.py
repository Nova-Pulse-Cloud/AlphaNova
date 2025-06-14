# File: core/signal_generation/alpha_signal_engine.py
# 🚀 Nova Pulse ALPHA – AlphaSignalEngine v2: Production Signal Orchestration

import os
import logging
import asyncio
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import openai
import aioredis
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer
from prometheus_client import Counter, Histogram, Gauge

from utils.config import Config
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.social_sentiment import score_token_sentiment, get_live_narrative
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.onchain_flows import get_token_flow_stats
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from utils.token_utils import get_token_rug_score
from core.alpha_memory_mesh import update_memory, get_token_memory_score
from core.signal_generation.reinforcement_tuner import DQNAgent
from utils.plugin_manager import fire_event
from fills_db import get_latest_fill_result  # RL feedback database

logger = logging.getLogger("nova.alpha_signal_engine_v2")
logger.setLevel(logging.INFO)

# Configuration and clients
config = Config()
openai.api_key = config.OPENAI_API_KEY
redis_client = None
solana_client = AsyncClient(config.HELIUS_RPC)

def _init_redis():
    global redis_client
    if not redis_client:
        redis_client = asyncio.get_event_loop().run_until_complete(
            aioredis.from_url(config.REDIS_URL, decode_responses=True)
        )

# Metrics
SIGNALS_GENERATED = Counter("signal_engine_signals_total", "Alpha signals generated")
SIGNAL_ERRORS     = Counter("signal_engine_errors_total", "Alpha signal generation errors")
OVERLAY_LATENCY   = Histogram("signal_engine_overlay_latency_seconds", "Signal overlay fetch latency", ["overlay"])
ACTION_GAUGE      = Gauge("signal_engine_action_gauge", "RL action output", ["action"])

# Plugin/event hooks
overlay_plugins: Dict[str, Any] = {}
event_hooks: List[Any] = []

register_overlay = lambda name, coro: overlay_plugins.update({name: coro})
register_event_hook = lambda fn: event_hooks.append(fn)

async def fire_event_async(event_name: str, data: dict):
    for hook in event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[SignalEngine] Event hook failure [{hook.__name__}]: {e}")

# ----------------------------
#   Trade Execution Helper
# ----------------------------

def load_keypair() -> Keypair:
    path = config.SOL_PRIVATE_KEY_PATH
    try:
        with open(path, 'r') as f:
            key_data = f.read().strip()
        secret = bytes.fromhex(key_data)
        return Keypair.from_secret_key(secret)
    except Exception as e:
        logger.error(f"Failed to load Solana key from {path}: {e}")
        raise

async def execute_trade(symbol: str, action: str, user: Optional[str] = None) -> Any:
    try:
        keypair = load_keypair()
        amount = 0.1  # adjust or parameterize
        tx = Transaction()
        tx.add(
            transfer(
                TransferParams(
                    from_pubkey=keypair.public_key,
                    to_pubkey=config.SOL_ADDRESS,
                    lamports=int(amount * 1e9)
                )
            )
        )
        resp = await solana_client.send_transaction(tx, keypair)
        logger.info(f"Trade executed for {symbol}: {resp}")
        pnl = calculate_pnl(symbol, action)
        emit_pnl_card(symbol, pnl, resp, user)
        return resp
    except Exception as e:
        logger.error(f"Trade execution failed for {symbol}: {e}")
        return {"status": "failed", "symbol": symbol, "action": action, "error": str(e)}

# ----------------------------
#   PNL Card Emission
# ----------------------------

def emit_pnl_card(symbol: str, pnl: float, details: Any, user: Optional[str] = None) -> None:
    card = {
        "symbol": symbol,
        "pnl": pnl,
        "details": details,
        "user": user,
        "timestamp": datetime.utcnow().isoformat()
    }
    asyncio.create_task(fire_event_async("pnl_card", card))
    logger.info(f"[PNL_CARD] {card}")

def calculate_pnl(symbol: str, action: str) -> float:
    # Placeholder - replace with live computation logic
    if action == "buy":
        return 0.05
    if action == "sell":
        return 0.10
    return 0.0

# ----------------------------
#   Main Signal Generation
# ----------------------------

async def generate_signal(symbol: str, user: Optional[str] = None) -> Dict[str, Any]:
    _init_redis()
    if not (await redis_client.get(f"auto_mode:{user}")):
        logger.info(f"Signals disabled. {symbol} skipped.")
        return {"symbol": symbol, "error": "OFF", "timestamp": datetime.utcnow().isoformat()}
    try:
        overlays: Dict[str, float] = {}
        for name, fn in overlay_plugins.items():
            start = datetime.utcnow()
            val = await fn(symbol, user)
            OVERLAY_LATENCY.labels(overlay=name).observe((datetime.utcnow()-start).total_seconds())
            overlays[name] = val
        imbalance = overlays.get("orderbook", 0)
        for k in overlays: overlays[k] *= (1 + imbalance)

        state = list(overlays.values())
        fill = get_latest_fill_result(symbol, user)
        agent = DQNAgent()
        action, q_vals = agent.act(state, feedback=fill)
        ACTION_GAUGE.labels(action=str(action)).set(q_vals[action])
        SIGNALS_GENERATED.inc()

        update_memory(symbol, overlays, action, user)
        expl = await generate_explainer(overlays, action, symbol)
        data = {"symbol": symbol, "action": action, "overlays": overlays, "explainer": expl, "timestamp": datetime.utcnow().isoformat()}
        await fire_event_async("signal_generated", data)

        if await redis_client.get(f"auto_mode:{user}"):
            return await execute_trade(symbol, action, user)
        return data
    except Exception as e:
        SIGNAL_ERRORS.inc()
        logger.error(f"Signal generation failed for {symbol}: {e}")
        return {"symbol": symbol, "error": str(e), "timestamp": datetime.utcnow().isoformat()}
