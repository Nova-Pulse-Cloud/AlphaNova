# File: core/signal_generation/alpha_signal_agent.py
# 🚀 Nova Pulse ALPHA – AlphaSignalAgent v2: Production Signal Fusion

import asyncio
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional
from uuid import uuid4

import openai
import aioredis
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from utils.config import Config
from utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.data_collection.orderbook_overlay import OrderBookOverlay
from utils.plugin_manager import fire_event  # centralized event bus

logger = logging.getLogger("nova.alpha_signal_agent_v2")
logger.setLevel(logging.INFO)

# Prometheus metrics
SIGNAL_TRIGGERS = Counter(
    "alpha_signal_triggers_total", "Total alpha signals triggered"
)
SIGNAL_ERRORS   = Counter(
    "alpha_signal_errors_total", "Total alpha signal errors"
)
SIGNAL_LATENCY  = Histogram(
    "alpha_signal_latency_seconds", "Alpha signal latency (seconds)"
)

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

class AlphaSignalAgent:
    """
    Nova ALPHA – AlphaSignalAgent v2
    Ultra-fast signal fusion: RL + trade memory weighting, Prometheus metrics,
    async event bus, LLM captioning, Redis DLQ, and provenance.
    """
    def __init__(
        self,
        config: Config,
        trade_memory: TradeMemory,
        rl_tuner: ReinforcementTuner,
        orderbook_overlay: OrderBookOverlay
    ):
        self.config = config
        self.redis_url = config.REDIS_URL
        self.openai_key = config.OPENAI_API_KEY
        self.trade_memory = trade_memory
        self.rl_tuner = rl_tuner
        self.orderbook_overlay = orderbook_overlay

        openai.api_key = self.openai_key
        self.redis = None

    async def connect(self):
        try:
            self.redis = await aioredis.from_url(
                self.redis_url, decode_responses=True
            )
            logger.info("[Nova] Connected to Redis.")
        except Exception as e:
            logger.critical(f"[Nova] Redis connection failure: {e}")
            raise

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            logger.info("[Nova] Redis disconnected.")

    async def _generate_caption(self, symbol: str, meta: dict) -> str:
        """
        Generate viral caption via OpenAI, fallback to basic.
        """
        prompt = (
            f"Write a viral trading signal caption for {symbol} with context: {meta}. Include emoji."
        )
        async def ask_openai():
            resp = await openai.ChatCompletion.acreate(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )
            return resp.choices[0].message.content

        try:
            # simple retry
            for attempt in range(2):
                try:
                    return await ask_openai()
                except Exception:
                    await asyncio.sleep(2 ** attempt)
            raise RuntimeError("Caption LLM failed after retries")
        except Exception:
            logger.warning(f"[Nova] LLM fallback caption for {symbol}")
            return f"Alpha signal for {symbol}! 🚀"

    async def evaluate(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_context: Optional[Dict] = None
    ) -> AlphaSignal:
        """
        Live fusion of multi-agent signals into a trusted alpha signal.
        """
        start = datetime.utcnow()
        SIGNAL_TRIGGERS.inc()
        try:
            # --- Trust-weighted scoring ---
            scores, trust_scores = {}, {}
            for agent in agent_signals:
                src = agent["source"]
                base = float(agent.get("score", 0))
                trust = self.rl_tuner.get_trust_score(src, symbol)
                memory = self.trade_memory.get_win_rate(symbol)
                weight = (trust + memory) / 2.0
                scores[src] = base * weight
                trust_scores[src] = weight

            # --- Market depth data ---
            ob = await self.orderbook_overlay.get_orderbook_data(symbol)
            liquidity = ob.get("liquidity", 0)
            whale_pressure = ob.get("whale_pressure", 0)
            market_adj = self._adjust_signal_based_on_market_depth(
                scores, liquidity, whale_pressure
            )

            # --- Final composite signal ---
            score = sum(scores.values()) / max(len(scores), 1) * market_adj
            risk = 1.0 - min(trust_scores.values()) if trust_scores else 1.0
            entry = float(agent_signals[0].get("entry", 0.0)) if agent_signals else 0.0
            flavor = self._choose_flavor(scores, user_context)
            meta = {
                "trust": trust_scores,
                "sources": list(scores.keys()),
                "market_adj": market_adj
            }
            caption = await self._generate_caption(symbol, meta)

            # --- Provenance hash ---
            prov_str = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}"
            prov_hash = hashlib.sha256(prov_str.encode()).hexdigest()

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

            await fire_event("alpha_signal", signal.dict())
            SIGNAL_LATENCY.observe((datetime.utcnow() - start).total_seconds())
            return signal

        except Exception as e:
            SIGNAL_ERRORS.inc()
            logger.error(f"[Nova][ALPHA][evaluate] Error: {e} | symbol={symbol}")
            raise

    def _choose_flavor(
        self,
        scores: Dict[str, float],
        user_context: Optional[Dict]
    ) -> str:
        avg = sum(scores.values()) / max(len(scores), 1)
        if user_context:
            prof = user_context.get("risk")
            if prof == "degen": return "Degen"
            if prof == "safe": return "Safe"
        if avg > 0.85: return "Safe"
        if avg < 0.3:  return "Degen"
        return "Moderate"

    def _adjust_signal_based_on_market_depth(
        self,
        scores: Dict[str, float],
        liquidity: float,
        whale_pressure: float
    ) -> float:
        factor = 1.0
        if liquidity < 0.1:
            factor *= 0.9
        if whale_pressure > 0.8:
            factor *= 1.2
        return factor
