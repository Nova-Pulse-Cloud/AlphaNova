# core/signal_generation/alpha_signal_agent.py
# 🚀 Nova Pulse ALPHA – AlphaSignalAgent v2.2: Ultra-Fast Production Signal Fusion

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import openai
import redis.asyncio as redis_client
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge

from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.data_collection.orderbook_overlay import OrderBookOverlay
from utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.alpha_signal_agent")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
SIGNAL_TRIGGERS = Counter(
    "alpha_signal_triggers_total", "Total alpha signals triggered"
)
SIGNAL_ERRORS = Counter(
    "alpha_signal_errors_total", "Total alpha signal errors"
)
SIGNAL_LATENCY = Histogram(
    "alpha_signal_latency_seconds", "Alpha signal latency in seconds"
)
SIGNAL_SCORE = Gauge(
    "alpha_signal_score", "Last generated alpha signal score", ["symbol"]
)

# ────────────────────────────────────────────────────────────────────────────────
# Pydantic Model
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
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

# ────────────────────────────────────────────────────────────────────────────────
# Agent
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignalAgent:
    def __init__(
        self,
        redis_url: str = config.REDIS_URL,
        openai_key: str = config.OPENAI_API_KEY,
        symbols: Optional[List[str]] = None,
    ):
        self.redis_url = redis_url
        openai.api_key = openai_key
        self.redis: Optional[redis_client.Redis] = None
        self.trade_memory = TradeMemory(config)
        self.rl_tuner = ReinforcementTuner()
        self.orderbook = OrderBookOverlay(config)
        self.symbols = symbols or []
        self._caption_lock = asyncio.Lock()

    async def connect(self):
        """Initialize Redis (DLQ storage, caching)."""
        if not self.redis:
            try:
                self.redis = await redis_client.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
                logger.info("[AlphaSignalAgent] Redis connected")
            except Exception as e:
                logger.error(f"[AlphaSignalAgent] Redis connect failed: {e}")
                self.redis = None

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            logger.info("[AlphaSignalAgent] Redis disconnected")

    async def _generate_caption(self, symbol: str, meta: Dict[str, Any]) -> str:
        prompt = f"Write a viral, concise trading signal caption for {symbol} with context {meta}."
        async with self._caption_lock:
            for attempt in range(2):
                try:
                    resp = await openai.ChatCompletion.acreate(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=32,
                        temperature=0.7,
                    )
                    return resp.choices[0].message.content.strip()
                except Exception as e:
                    logger.warning(f"[AlphaSignalAgent] Caption attempt {attempt} failed: {e}")
                    await asyncio.sleep(2 ** attempt)
            return f"{symbol} signal generated! 🚀"

    def _compute_provenance(
        self, symbol: str, scores: Dict[str, float], meta: Dict[str, Any], flavor: str, score: float, risk: float, entry: float
    ) -> str:
        blob = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}"
        return hashlib.sha256(blob.encode()).hexdigest()

    def _choose_flavor(self, avg_score: float, user_context: Optional[Dict]) -> str:
        if user_context:
            risk_pref = user_context.get("risk")
            if risk_pref == "degen":
                return "Degen"
            if risk_pref == "safe":
                return "Safe"
        if avg_score > 0.85:
            return "Safe"
        if avg_score < 0.3:
            return "Degen"
        return "Moderate"

    async def evaluate(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_context: Optional[Dict[str, Any]] = None,
    ) -> AlphaSignal:
        """
        Fuse multi-agent scores into one alpha signal with:
        - RL trust weighting
        - Trade-memory weighting
        - Market-depth adjustment
        - LLM captioning
        - Provenance hashing
        - Prometheus metrics
        """
        SIGNAL_TRIGGERS.inc()
        start = datetime.utcnow()

        try:
            # 1) Trust + memory weights
            weighted_scores = {}
            trust_map = {}
            memory_rate = await asyncio.get_event_loop().run_in_executor(
                None, self.trade_memory.get_win_rate, symbol
            )
            for sig in agent_signals:
                src = sig["source"]
                base = float(sig.get("score", 0.0))
                trust = self.rl_tuner.get_trust_score(src, symbol)
                weight = (trust + memory_rate) / 2.0
                weighted_scores[src] = base * weight
                trust_map[src] = weight

            # 2) Market-depth overlays
            ob = await self.orderbook.get_orderbook_data(symbol)
            liquidity = ob.get("liquidity", 0.0)
            whale_pressure = ob.get("whale_pressure", 0.0)
            depth_adj = 0.9 if liquidity < 0.1 else 1.0
            if whale_pressure > 0.8:
                depth_adj *= 1.1

            # 3) Composite score & risk
            avg = sum(weighted_scores.values()) / max(len(weighted_scores), 1)
            score = avg * depth_adj
            risk = 1.0 - min(trust_map.values()) if trust_map else 1.0
            entry = float(agent_signals[0].get("entry", 0.0)) if agent_signals else 0.0

            # 4) Flavor & caption
            flavor = self._choose_flavor(avg, user_context)
            caption = await self._generate_caption(symbol, {"avg": avg, "depth_adj": depth_adj})

            # 5) Provenance
            prov_hash = self._compute_provenance(symbol, weighted_scores, {"depth_adj": depth_adj}, flavor, score, risk, entry)

            signal = AlphaSignal(
                symbol=symbol,
                trigger_sources=weighted_scores,
                meta_features={"trusts": trust_map, **ob},
                flavor=flavor,
                score=round(score, 4),
                risk=round(risk, 4),
                entry=round(entry, 6),
                caption=caption,
                provenance_hash=prov_hash,
                timestamp=int(start.timestamp()),
            )

            SIGNAL_SCORE.labels(symbol=symbol).set(score)
            SIGNAL_LATENCY.observe((datetime.utcnow() - start).total_seconds())

            # 6) Emit event for downstream hooks (execution, logging, telegram)
            await fire_plugin_event("alpha_signal", signal.dict())
            return signal

        except Exception as e:
            SIGNAL_ERRORS.inc()
            logger.error(f"[AlphaSignalAgent] evaluate error for {symbol}: {e}")
            raise

# singleton
alpha_signal_agent = AlphaSignalAgent()
