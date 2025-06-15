# core/signal_generation/alpha_signal_agent.py
"""
Nova Pulse ALPHA – AlphaSignalAgent v3.0: Production Signal Fusion & Auto-Trading
───────────────────────────────────────────────────────────────────────────────
• Async signal fusion: RL-tuner + memory weighting + orderbook depth
• Redis-backed auto-trade flag and DLQ for failed captions
• Prometheus metrics, OpenTelemetry tracing
• Event bus via PluginManager
• Telegram notification + optional auto execution
"""
import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import openai
import redis.asyncio as redis_client
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram
from opentelemetry import trace

from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.data_collection.orderbook_overlay import OrderBookOverlay
from core.execution.execution_router import ExecutionRouter
from utils.plugin_manager import fire_plugin_event
from telegram.integration.telegram_router import push_signal_update

logger = logging.getLogger("nova.alpha_signal_agent")
logger.setLevel(logging.INFO)
tracer = trace.get_tracer(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
SIG_TRIGGERS = Counter(
    "alpha_signal_triggers_total", "Total alpha signals generated", ["user"]
)
SIG_ERRORS   = Counter(
    "alpha_signal_errors_total", "Errors in alpha signal generation", ["user"]
)
SIG_LATENCY  = Histogram(
    "alpha_signal_latency_seconds", "Latency per alpha signal (s)", ["user"]
)

# ────────────────────────────────────────────────────────────────────────────────
# Pydantic Model
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    trigger_scores: Dict[str, float]
    meta_features: Dict[str, Any]
    flavor: str
    score: float
    risk: float
    entry: float
    caption: str
    timestamp: int
    provenance_hash: str

class AlphaSignalAgent:
    def __init__(
        self,
        redis_url: str = config.REDIS_URL,
        openai_key: str = config.OPENAI_API_KEY,
        orderbook_overlay: OrderBookOverlay = None,
        rl_tuner: ReinforcementTuner = None,
        trade_memory: TradeMemory = None,
        execution_router: ExecutionRouter = None,
    ):
        self.redis_url = redis_url
        self.openai_key = openai_key
        openai.api_key = openai_key
        self.redis: Optional[redis_client.Redis] = None
        self.trade_memory = trade_memory or TradeMemory(config)
        self.rl_tuner = rl_tuner or ReinforcementTuner()
        self.orderbook = orderbook_overlay or OrderBookOverlay(config)
        self.exec_router = execution_router or ExecutionRouter(
            wallet_manager=None,  # inject your WalletManager
            rpc_nodes=config.RPC_NODES
        )

    async def _connect_redis(self):
        if not self.redis:
            try:
                self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
                logger.info("[AlphaSignalAgent] Redis connected")
            except Exception as e:
                logger.error(f"[AlphaSignalAgent] Redis connect failed: {e}")
                self.redis = None

    async def evaluate(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_id: Optional[str] = None,
        timeout: float = 5.0
    ) -> AlphaSignal:
        """Fuse multi-agent signals into a single AlphaSignal, notify Telegram & optionally auto-trade."""
        SIG_TRIGGERS.labels(user=user_id or "global").inc()
        start = time.time()
        trace_ctx = tracer.start_as_current_span("alpha_signal")
        with tracer.use_span(trace_ctx, end_on_exit=False):
            try:
                # 1️⃣ Trust-weighted fusion
                scores, trusts = {}, {}
                for ag in agent_signals:
                    src = ag["source"]
                    base = float(ag.get("score", 0.0))
                    trust = self.rl_tuner.get_trust_score(src, symbol)
                    memory = self.trade_memory.get_win_rate(symbol) or 0.0
                    w = (trust + memory) / 2.0
                    scores[src] = base * w
                    trusts[src] = w

                # 2️⃣ Market depth overlay
                ob = await asyncio.wait_for(self.orderbook.analyze(symbol), timeout=timeout)
                depth_adj = 1.0
                if ob.get("liquidity", 1.0) < 0.1:
                    depth_adj *= 0.9
                if ob.get("whale_pressure", 0.0) > 0.8:
                    depth_adj *= 1.2

                # 3️⃣ Composite score & meta
                score = sum(scores.values()) / max(len(scores), 1) * depth_adj
                risk  = 1.0 - min(trusts.values()) if trusts else 1.0
                entry = float(agent_signals[0].get("entry", 0.0))
                flavor = self._choose_flavor(score, risk, user_id)
                meta = {"trusts": trusts, "depth_adj": depth_adj, **ob}

                # 4️⃣ Caption (LLM) with retry
                caption = await self._gen_caption(symbol, meta, timeout=timeout)

                # 5️⃣ Provenance
                prov = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}"
                prov_hash = hashlib.sha256(prov.encode()).hexdigest()

                signal = AlphaSignal(
                    symbol=symbol,
                    trigger_scores=scores,
                    meta_features=meta,
                    flavor=flavor,
                    score=score,
                    risk=risk,
                    entry=entry,
                    caption=caption,
                    timestamp=int(datetime.utcnow().timestamp()),
                    provenance_hash=prov_hash,
                )

                # 6️⃣ Fire event & Telegram
                await fire_plugin_event("alpha_signal", signal.dict())
                await push_signal_update(signal.dict(), user_id=user_id)

                # 7️⃣ Auto-trade if enabled
                await self._auto_trade_if_enabled(signal, user_id)

                return signal

            except Exception as e:
                SIG_ERRORS.labels(user=user_id or "global").inc()
                logger.error(f"[AlphaSignalAgent] evaluation error for {symbol}: {e}", exc_info=True)
                raise

            finally:
                SIG_LATENCY.labels(user=user_id or "global").observe(time.time() - start)
                trace_ctx.end()

    async def _gen_caption(self, symbol: str, meta: dict, timeout: float) -> str:
        prompt = f"Create a viral trading caption for {symbol} with meta {meta}"
        for attempt in range(2):
            try:
                resp = await asyncio.wait_for(
                    openai.ChatCompletion.acreate(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=64,
                    ),
                    timeout=timeout
                )
                return resp.choices[0].message.content.strip()
            except Exception:
                await asyncio.sleep(2 ** attempt)
        return f"{symbol} signal generated 🚀"

    async def _auto_trade_if_enabled(self, signal: AlphaSignal, user_id: Optional[str]):
        await self._connect_redis()
        if not self.redis or not user_id:
            return
        flag = await self.redis.get(f"auto:{user_id}")
        if flag and flag.lower() in ("1", "true", "on"):
            trade = {
                "symbol": signal.symbol,
                "side": "buy" if signal.score >= 0 else "sell",
                "score": signal.score,
                "entry_price": signal.entry,
                "risk": signal.risk,
                "flavor": signal.flavor,
                "tp": signal.entry * (1 + signal.score * 0.1),
                "sl": signal.entry * (1 - signal.score * 0.05),
                "size": abs(signal.score) * config.DEFAULT_ORDER_SIZE,
                "trace_id": signal.id,
            }
            # fire and forget
            asyncio.create_task(self.exec_router.execute_trade(trade, user_id=user_id))

    def _choose_flavor(self, score: float, risk: float, user_id: Optional[str]) -> str:
        if score > 0.85: return "Safe"
        if score < 0.30: return "Degen"
        return "Moderate"
