# core/signal_generation/combined_signal_engine.py

import asyncio
import hashlib
import json
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import openai
import redis.asyncio as redis_async
import numpy as np
from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Counter, Histogram, Gauge
from solana.rpc.async_api import AsyncClient

from core.signal_generation.reinforcement_tuner import ReinforcementTuner, DQNAgent
from core.data_collection.market_conditions import MarketConditionsService
from core.data_collection.social_sentiment import score_token_sentiment, get_live_narrative
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from core.data_collection.onchain_flows import get_token_flow_stats
from core.utils.token_utils import TokenUtils
from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.alpha_memory_mesh import update_memory, get_token_memory_score
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from utils.plugin_manager import fire_plugin_event
from utils.token_utils import TokenUtils

logger = logging.getLogger("nova.combined_signal_engine")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
SIG_TRIGGERS = Counter("signal_triggers_total",  "Total composite signal generations")
SIG_ERRORS   = Counter("signal_errors_total",    "Errors in composite signal engine")
SIG_LATENCY  = Histogram("signal_latency_seconds","Latency of composite signal generation")
SIG_ALERTS   = Counter("signal_alerts_total",     "Alerts from composite engine")
SIG_ACTION   = Gauge("signal_action_gauge",       "RL action values", ["action"])

# ────────────────────────────────────────────────────────────────────────────────
# Redis & OpenAI Init
# ────────────────────────────────────────────────────────────────────────────────
redis_client: Optional[redis_async.Redis] = None

async def get_redis() -> redis_async.Redis:
    global redis_client
    if not redis_client:
        redis_client = await redis_async.from_url(config.REDIS_URL, decode_responses=True)
    return redis_client

openai.api_key = config.OPENAI_API_KEY
solana_client = AsyncClient(config.HELIUS_RPC)

# ────────────────────────────────────────────────────────────────────────────────
# Data Models
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    score: float
    risk: float
    entry: float
    flavor: str
    trigger_sources: Dict[str, float]
    meta_features: Dict[str, Any]
    caption: str
    provenance_hash: str
    timestamp: int

# ────────────────────────────────────────────────────────────────────────────────
# CombinedSignalEngine
# ────────────────────────────────────────────────────────────────────────────────
class CombinedSignalEngine:
    def __init__(
        self,
        trade_memory: TradeMemory,
        rl_tuner: ReinforcementTuner,
        adaptive_tp_sl: AdaptiveTPSL,
        market_svc: Optional[MarketConditionsService] = None,
    ):
        self.trade_memory = trade_memory
        self.rl_tuner = rl_tuner
        self.adaptive_tp_sl = adaptive_tp_sl
        self.market_svc = market_svc or MarketConditionsService(config)
        self.event_hooks: List[Any] = []

    def register_event_hook(self, fn: Any) -> None:
        self.event_hooks.append(fn)

    async def fire_event(self, event: str, data: Any) -> None:
        for hook in self.event_hooks:
            try:
                await hook(event, data)
            except Exception:
                logger.warning(f"[CombinedEngine] Event hook {hook.__name__} failed")
        await fire_plugin_event(event, data)

    async def evaluate_alpha(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_context: Optional[Dict[str, Any]] = None,
    ) -> AlphaSignal:
        SIG_TRIGGERS.inc()
        start = datetime.utcnow()
        try:
            # 1) Fuse agent signals with trust & memory weights
            scores = {}
            trusts = {}
            mem_score = await asyncio.get_running_loop().run_in_executor(
                None, self.trade_memory.win_rate, symbol
            )
            for sig in agent_signals:
                src = sig.get("source")
                base = float(sig.get("score", 0))
                trust = await self.rl_tuner.get_trust_score(src, symbol)
                weight = (trust + mem_score) / 2
                scores[src] = base * weight
                trusts[src] = weight

            # 2) Market overlays in parallel
            ob_task     = asyncio.create_task(get_orderbook_imbalance(symbol))
            whale_task  = asyncio.create_task(get_recent_whale_activity(symbol))
            flow_task   = asyncio.create_task(get_token_flow_stats(symbol))
            ob_data, whale_data, flow_data = await asyncio.gather(ob_task, whale_task, flow_task)

            # 3) Composite score & risk
            depth_adj = 1.0 - min(ob_data.get("imbalance_score", 0), 1)
            whale_adj = whale_data.get("score", 0)
            composite = sum(scores.values()) / max(len(scores), 1)
            score = composite * (1 + whale_adj) * depth_adj
            risk  = 1 - min(trusts.values()) if trusts else 1
            entry = float(agent_signals[0].get("entry", 0)) if agent_signals else 0
            flavor= self._choose_flavor(score, user_context)

            # 4) Caption with OpenAI (timeout guard)
            caption = await self._gen_caption(symbol, scores)  

            # 5) Adaptive TP/SL
            tp_sl = await self.adaptive_tp_sl.compute(symbol, entry, {**{
                "score":score,
                **ob_data,
                **whale_data,
                **flow_data
            }})

            # 6) Build signal
            prov = f"{symbol}{scores}{flavor}{score}{risk}{entry}"
            sig = AlphaSignal(
                symbol=symbol,
                score=score,
                risk=risk,
                entry=entry,
                flavor=flavor,
                trigger_sources=scores,
                meta_features={"depth_adj":depth_adj, **whale_data},
                caption=caption,
                provenance_hash=hashlib.sha256(prov.encode()).hexdigest(),
                timestamp=int(start.timestamp()),
            )

            await self.fire_event("alpha_signal", sig.dict())
            SIG_LATENCY.observe((datetime.utcnow() - start).total_seconds())
            return sig

        except Exception as e:
            SIG_ERRORS.inc()
            logger.error(f"[CombinedEngine] evaluate_alpha error: {e}")
            raise

    async def generate_signal(
        self,
        symbol: str,
        user: Optional[str] = None
    ) -> Dict[str, Any]:
        SIG_TRIGGERS.inc()
        start = datetime.utcnow()
        try:
            # Gather overlays
            overlays = {
                "volatility": await self.market_svc.get_volatility(symbol),
                "trend":      await self.market_svc.get_trend_strength(symbol),
                "sentiment":  await score_token_sentiment(symbol),
                "whale":      (await get_recent_whale_activity(symbol)).get("score",0),
            }

            # RL action
            agent = DQNAgent()
            q_values = agent.act(list(overlays.values()))
            action = max(q_values, key=q_values.get)
            SIG_ACTION.labels(action=str(action)).set(q_values[action])

            # Compose final package
            sig = await self.evaluate_alpha(symbol, [{"source":"rl_agent","score":q_values[action],"entry":overlays.get("volatility",0)}])
            result = sig.dict()

            # Auto-trade if enabled
            if user and await (await get_redis()).get(f"auto:{user}"):  
                from core.execution.execution_router import ExecutionRouter
                router = ExecutionRouter(...)
                asyncio.create_task(router.execute_trade(result, user))

            SIG_LATENCY.observe((datetime.utcnow() - start).total_seconds())
            return result

        except Exception as e:
            SIG_ERRORS.inc()
            logger.error(f"[CombinedEngine] generate_signal error: {e}")
            return {"error": str(e)}

    def _choose_flavor(self, score: float, ctx: Optional[Dict[str, Any]]) -> str:
        if ctx and ctx.get("risk") == "degen": return "Degen"
        if score > 0.9:  return "Safe"
        if score < 0.3:  return "Degen"
        return "Moderate"

    async def _gen_caption(self, symbol: str, scores: Dict[str, float]) -> str:
        prompt = f"Caption for {symbol} with scores {scores}"  
        try:
            resp = await asyncio.wait_for(
                openai.ChatCompletion.acreate(
                    model="gpt-4-turbo", messages=[{"role":"user","content":prompt}], max_tokens=50
                ),
                timeout=3.0
            )
            return resp.choices[0].message.content.strip()
        except Exception:
            return f"Nova Pulse {symbol} signal 🚀"

# Utility

def combine_ensemble(comps: List[Any]) -> Dict[str, Any]:
    if not comps:
        return {"signal_strength":0.0, "alert":False, "note":"No data"}
    scores = [c[1].get("signal_strength",0) for c in comps]
    avg = float(np.mean(scores))
    return {"signal_strength":avg, "alert":avg>0.85, "note":"Ensemble"}
