# core/signal_generation/combined_signal_engine.py

import asyncio
import hashlib
import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge

from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from core.data_collection.social_sentiment import SocialSentiment
from core.data_collection.whale_detector import get_recent_whale_activity
from core.data_collection.onchain_flows import get_token_flow_stats
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.signal_generation.reinforcement_learning import RLTrader
from core.agents.adaptive_tp_sl import AdaptiveTPSL
from core.utils.config import config
from core.utils.plugin_manager import fire_plugin_event
from core.utils.trade_memory import TradeMemory
from core.wallet_management.wallet_manager import WalletManager
from core.execution.execution_router import ExecutionRouter

logger = logging.getLogger("nova.combined_signal_engine")
logger.setLevel(logging.INFO)

# ───────────────────────────────────────────────────────
SIG_TRIGGERS = Counter("signal_triggers_total",  "Total composite signal gens")
SIG_ERRORS   = Counter("signal_errors_total",    "Composite signal errors")
SIG_LATENCY  = Histogram("signal_latency_seconds","Composite signal latency")
SIG_ACTION   = Gauge("signal_action_gauge",       "RL action values", ["action"])

_sentiment = SocialSentiment(config)

class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    symbol: str
    score: float
    risk: float
    entry: float
    tp: float
    sl: float
    flavor: str
    trigger_sources: Dict[str, float]
    meta_features: Dict[str, Any]
    caption: str
    provenance_hash: str
    timestamp: int

class CombinedSignalEngine:
    def __init__(
        self,
        trade_memory: TradeMemory,
        wallet_manager: WalletManager,
        rl_tuner: ReinforcementTuner,
        adaptive_tp_sl: AdaptiveTPSL,
    ):
        self.trade_memory   = trade_memory
        self.wallet_manager = wallet_manager
        self.rl_tuner       = rl_tuner
        self.adaptive_tp_sl = adaptive_tp_sl
        self.event_hooks: List[Any] = []

    def register_event_hook(self, fn: Any) -> None:
        self.event_hooks.append(fn)

    async def _fire_event(self, event: str, data: Any) -> None:
        for hook in self.event_hooks:
            try:
                await hook(event, data)
            except Exception:
                logger.warning(f"[CombinedEngine] hook {hook.__name__} failed")
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
            scores, trusts = {}, {}
            mem_rate = await asyncio.get_event_loop().run_in_executor(
                None, self.trade_memory.get_win_rate, symbol
            )
            for sig in agent_signals:
                src    = sig["source"]
                base   = float(sig.get("score", 0.0))
                trust  = self.rl_tuner.get_trust_score(src, symbol)
                weight = (trust + mem_rate) / 2
                scores[src] = base * weight
                trusts[src] = weight

            ob_task    = asyncio.create_task(get_orderbook_imbalance(symbol))
            whale_task = asyncio.create_task(get_recent_whale_activity(symbol))
            flow_task  = asyncio.create_task(get_token_flow_stats(symbol))
            ob, whale, flow = await asyncio.gather(ob_task, whale_task, flow_task)

            sent_score   = await _sentiment.score_token_sentiment(symbol)
            caption_narr = await _sentiment.get_live_narrative(symbol)

            depth_adj = 1.0 - min(abs(ob), 1.0)
            whale_adj = whale.get("score", 0.0)
            composite = sum(scores.values()) / max(len(scores),1)
            score     = composite * (1 + whale_adj) * depth_adj
            risk      = 1 - min(trusts.values()) if trusts else 1.0
            entry     = float(agent_signals[0].get("entry", 0.0)) if agent_signals else 0.0
            flavor    = "Safe" if score > 0.8 else "Moderate" if score > 0.3 else "Degen"

            tp_sl = await self.adaptive_tp_sl.compute(
                symbol,
                entry,
                {"volatility": ob, "whale_pressure": whale_adj, "imbalance": flow.get("imbalance", 0.0)},
            )
            tp = float(tp_sl["tp"])
            sl = float(tp_sl["sl"])

            blob = f"{symbol}{scores}{flavor}{score}{risk}{entry}{tp}{sl}"
            sig = AlphaSignal(
                symbol=symbol,
                score=round(score,4),
                risk=round(risk,4),
                entry=round(entry,6),
                tp=tp,
                sl=sl,
                flavor=flavor,
                trigger_sources=scores,
                meta_features={**tp_sl, "sentiment_score": sent_score},
                caption=caption_narr,
                provenance_hash=hashlib.sha256(blob.encode()).hexdigest(),
                timestamp=int(start.timestamp()),
            )

            await self._fire_event("alpha_signal", sig.dict())
            SIG_LATENCY.observe((datetime.utcnow() - start).total_seconds())
            return sig

        except Exception as e:
            SIG_ERRORS.inc()
            logger.error(f"[CombinedEngine] evaluate_alpha error: {e}", exc_info=True)
            raise

    async def generate_signal(
        self,
        symbol: str,
        user: Optional[str] = None
    ) -> Dict[str, Any]:
        SIG_TRIGGERS.inc()
        try:
            sent  = await _sentiment.score_token_sentiment(symbol)
            agent = RLTrader()
            action = await agent.act({"score": sent})
            SIG_ACTION.labels(action=action.action).set(action.size)

            sig_res = await self.evaluate_alpha(
                symbol,
                [{"source":"rl_agent","score":action.size,"entry":sent}]
            )
            result = sig_res.dict()

            if user and await _sentiment._redis.get(f"auto:{user}") == "1":
                router = ExecutionRouter(wallet_manager=self.wallet_manager, rpc_nodes=[config.HELIUS_RPC])
                asyncio.create_task(router.execute_trade(result, user))

            return result

        except Exception as e:
            SIG_ERRORS.inc()
            return {"error": str(e)}
