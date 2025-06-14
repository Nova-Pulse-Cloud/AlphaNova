# File: core/signal_generation/combined_signal_engine.py
# 🚀 Nova Pulse ALPHA – Unified Signal Engine: Fusion & Orchestration

import asyncio
import logging
import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable

import openai
import aioredis
import numpy as np
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge
from solana.rpc.async_api import AsyncClient
from solders.keypair import Keypair
from solana.transaction import Transaction
from solana.system_program import TransferParams, transfer

from utils.config import Config
from utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner, DQNAgent
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.social_sentiment import score_token_sentiment, get_live_narrative
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.onchain_flows import get_token_flow_stats
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from utils.token_utils import get_token_rug_score
from core.alpha_memory_mesh import update_memory, get_token_memory_score
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from utils.plugin_manager import fire_event  # Unified event bus
from fills_db import get_latest_fill_result  # RL feedback

logger = logging.getLogger("nova.combined_signal_engine")
logger.setLevel(logging.INFO)

# === Config & Clients ===
config = Config()
openai.api_key = config.OPENAI_API_KEY
redis_client = None
solana_client = AsyncClient(config.HELIUS_RPC)

# === Metrics ===
SIG_TRIGGERS = Counter("signal_triggers_total", "Total signals triggered")
SIG_ERRORS   = Counter("signal_errors_total",   "Signal errors")
SIG_LATENCY  = Histogram("signal_latency_seconds", "Signal latency")
SIG_ALERTS   = Counter("signal_alerts_total",    "Signal alerts")
SIG_ACTION   = Gauge("signal_action_gauge",      "RL action output", ["action"])

# === Shared Utilities ===

def init_redis():
    global redis_client
    if not redis_client:
        redis_client = asyncio.get_event_loop().run_until_complete(
            aioredis.from_url(config.REDIS_URL, decode_responses=True)
        )

# === Data Models ===
class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    trigger_sources: Dict[str, float]
    meta_features: Dict[str, Any]
    flavor: str
    score: float
    risk: float
    entry: float
    caption: str
    provenance_hash: str
    timestamp: int

# === Engine ===
class CombinedSignalEngine:
    def __init__(
        self,
        trade_memory: TradeMemory,
        rl_tuner: ReinforcementTuner,
        adaptive_tp_sl: AdaptiveTPSL
    ):
        self.trade_memory = trade_memory
        self.rl_tuner = rl_tuner
        self.adaptive_tp_sl = adaptive_tp_sl
        self.event_hooks: List[Callable] = []
        self.overlay_plugins: Dict[str, Callable] = {}
        self.signal_plugins: Dict[str, Callable] = {}

    def register_event_hook(self, fn: Callable):
        self.event_hooks.append(fn)

    def register_overlay(self, name: str, fn: Callable):
        self.overlay_plugins[name] = fn

    def register_signal_plugin(self, name: str, fn: Callable):
        self.signal_plugins[name] = fn

    async def fire_event(self, event: str, data: Any):
        for hook in self.event_hooks:
            try:
                await hook(event, data)
            except Exception as e:
                logger.error(f"[Event] Hook failure {hook}: {e}")

    async def evaluate_alpha(self, symbol: str, agent_signals: List[Dict[str, Any]], user_context: Optional[Dict] = None) -> AlphaSignal:
        """
        Deep fusion: trust-weighted scoring + market-depth + LLM caption + provenance
        """
        SIG_TRIGGERS.inc()
        start = datetime.utcnow()
        try:
            # Trust + memory weights
            scores, trusts = {}, {}
            for s in agent_signals:
                src, base = s["source"], float(s["score"])
                trust = self.rl_tuner.get_trust_score(src, symbol)
                memory = self.trade_memory.get_win_rate(symbol)
                weight = (trust + memory) / 2
                scores[src] = base * weight
                trusts[src] = weight

            # Market depth overlays
            liquidity, whale = await get_orderbook_imbalance(symbol), await get_recent_whale_activity(symbol)
            depth_adj = 1.0
            if liquidity < 0.1: depth_adj *= 0.9
            if whale > 0.8: depth_adj *= 1.2

            # Composite score
            score = sum(scores.values())/max(len(scores),1) * depth_adj
            risk  = 1 - min(trusts.values()) if trusts else 1
            entry = float(agent_signals[0].get("entry", 0))
            flavor= self._choose_flavor(scores, user_context)
            meta  = {"trusts": trusts, "depth_adj": depth_adj}

            # Caption via LLM
            prompt = f"Caption for {symbol} with meta {meta}"
            caption= await self._gen_caption(prompt)

            # Provenance
            prov_str = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}"
            prov_hash= hashlib.sha256(prov_str.encode()).hexdigest()

            signal = AlphaSignal(
                symbol=symbol,
                trigger_sources=scores,
                meta_features=meta,
                flavor=flavor,
                score=score,
                risk=risk,
                entry=entry,
                caption=caption,
                provenance_hash=prov_hash,
                timestamp=int(start.timestamp())
            )
            await self.fire_event("alpha_signal", signal.dict())
            SIG_LATENCY.observe((datetime.utcnow()-start).total_seconds())
            return signal
        except Exception as e:
            SIG_ERRORS.inc()
            logger.error(f"Alpha eval error: {e}")
            raise

    async def generate_signal(self, symbol: str, user: Optional[str] = None) -> Dict[str, Any]:
        """
        Orchestrator: overlay fetch, RL action, ensemble combine, adaptive TP/SL, optional execution
        """
        init_redis()
        auto = await redis_client.get(f"auto:{user}")
        SIG_TRIGGERS.inc()
        start = datetime.utcnow()
        try:
            # Overlays
            overlays={}
            for n, fn in self.overlay_plugins.items():
                overlays[n] = await fn(symbol)

            # RL action
            state = list(overlays.values())
            fill  = await get_latest_fill_result(symbol, user)
            agent= DQNAgent()
            action,qv= agent.act(state, feedback=fill)
            SIG_ACTION.labels(action=str(action)).set(qv[action])

            # Ensemble or default
            if self.signal_plugins:
                comps=[(n, await fn(symbol)) for n,fn in self.signal_plugins.items()]
                composite=combine_ensemble(comps)
            else:
                composite=self._default_signal(overlays)

            # TP/SL
            tp_sl= await self.adaptive_tp_sl.compute(symbol, overlays.get("price"), composite)
            composite.update(tp_sl)

            result={"symbol":symbol,"action":action,"composite":composite}
            await self.fire_event("signal_generated", result)

            if auto:
                from core.execution_router import ExecutionRouter
                # dispatch trade via execution_router
            SIG_LATENCY.observe((datetime.utcnow()-start).total_seconds())
            return result
        except Exception as e:
            SIG_ERRORS.inc()
            logger.error(f"Signal gen error: {e}")
            return {"error":str(e)}

    def _choose_flavor(self, scores: Dict[str, float], ctx: Optional[Dict]) -> str:
        avg=sum(scores.values())/max(len(scores),1)
        if ctx and ctx.get("risk")=="degen": return "Degen"
        if ctx and ctx.get("risk")=="safe": return "Safe"
        if avg>0.85: return "Safe"
        if avg<0.3:  return "Degen"
        return "Moderate"

    async def _gen_caption(self, prompt: str) -> str:
        try:
            resp=await openai.ChatCompletion.acreate(model="gpt-3.5-turbo",messages=[{"role":"user","content":prompt}])
            return resp.choices[0].message.content
        except:
            return "Nova Pulse signal 🚀"

    def _default_signal(self, overlays: Dict[str,float]) -> Dict[str, Any]:
        composite=sum(overlays.values())/max(len(overlays),1)
        return {"signal_strength":composite,"alert":composite>0.85,"note":"Hold"}

    async def execute_trade(self, symbol:str, action:str, user:Optional[str]=None):
        # Use your unified executor here
        pass
