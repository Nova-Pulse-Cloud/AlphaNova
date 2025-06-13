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

from core.trade_memory import TradeMemory
from core.reinforcement_learning import ReinforcementTuner
from core.orderbook_overlay import OrderBookOverlay  # Import OrderBookOverlay

logger = logging.getLogger("nova.alpha_signal_agent_v2")
logger.setLevel(logging.INFO)

# Prometheus metrics
SIGNAL_TRIGGERS = Counter("alpha_signal_triggers_total", "Total alpha signals triggered")
SIGNAL_ERRORS = Counter("alpha_signal_errors_total", "Total alpha signal errors")
SIGNAL_LATENCY = Histogram("alpha_signal_latency_seconds", "Alpha signal latency (seconds)")

# Event system (live hooks for all downstream plugins)
event_hooks = []
def register_event_hook(hook_fn): event_hooks.append(hook_fn)

async def fire_event(event_name: str, data: dict):
    for hook in event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[Nova] Event hook failure [{hook.__name__}]: {e}")

async def retry_async(fn, retries=3, delay=2, err_msg=None):
    for attempt in range(retries):
        try:
            return await fn()
        except Exception as e:
            if attempt == retries - 1:
                logger.error(f"{err_msg or 'Retry failed'}: {e}")
                raise
            await asyncio.sleep(delay * (2 ** attempt))

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
    Ultra-fast signal fusion. RL + trade memory trust weighting, Prometheus metrics, async event bus,
    LLM captioning, Redis DLQ, and total provenance.
    """
    def __init__(
        self,
        redis_url: str,
        openai_key: str,
        trade_memory: TradeMemory,
        rl_tuner: ReinforcementTuner,
        orderbook_overlay: OrderBookOverlay  # Inject OrderBookOverlay for market depth integration
    ):
        self.redis_url = redis_url
        self.openai_key = openai_key
        self.redis = None
        self.trade_memory = trade_memory
        self.rl_tuner = rl_tuner
        self.orderbook_overlay = orderbook_overlay  # OrderBookOverlay instance
        openai.api_key = openai_key

    async def connect(self):
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
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
        Generate viral caption via OpenAI, fallback to basic if LLM fails.
        """
        prompt = f"Write a viral trading signal caption for {symbol} with context: {meta}. Include emoji."
        async def ask_openai():
            return openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}]
            )["choices"][0]["message"]["content"]

        try:
            return await retry_async(ask_openai, retries=2, delay=2, err_msg="Caption LLM failed")
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
        Live fusion of multi-agent signals into trust-weighted alpha signal.
        Incorporates market depth (order book) for decision-making.
        """
        async with SIGNAL_LATENCY.time():
            SIGNAL_TRIGGERS.inc()
            try:
                # Trust-weighted scoring
                scores, trust_scores = {}, {}
                for agent in agent_signals:
                    src = agent["source"]
                    base = float(agent["score"])
                    trust = float(self.rl_tuner.get_trust_score(src, symbol))
                    memory = float(self.trade_memory.get_win_rate(symbol, src))
                    weight = (trust + memory) / 2.0
                    scores[src] = base * weight
                    trust_scores[src] = weight

                # Fetch real-time market data (Order Book Data)
                orderbook_data = await self.orderbook_overlay.get_orderbook_data(symbol)
                liquidity = orderbook_data.get("liquidity", 0)
                whale_pressure = orderbook_data.get("whale_pressure", 0)

                # Factor market depth into the final signal
                market_adjustment = self._adjust_signal_based_on_market_depth(scores, liquidity, whale_pressure)

                score = sum(scores.values()) / max(len(scores), 1) * market_adjustment
                risk = 1.0 - min(trust_scores.values()) if trust_scores else 1.0
                entry = float(agent_signals[0].get("entry", 0.0)) if agent_signals else 0.0
                flavor = self._choose_flavor(scores, user_context)
                meta = {"trust": trust_scores, "sources": list(scores.keys()), "market_adjustment": market_adjustment}
                caption = await self._generate_caption(symbol, meta)

                # Provenance: SHA256 hash for full signal traceability
                provenance_input = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}"
                provenance_hash = hashlib.sha256(provenance_input.encode()).hexdigest()

                alpha_signal = AlphaSignal(
                    symbol=symbol,
                    trigger_sources=scores,
                    meta_features=meta,
                    flavor=flavor,
                    score=score,
                    risk=risk,
                    entry=entry,
                    caption=caption,
                    provenance_hash=provenance_hash,
                    timestamp=int(datetime.utcnow().timestamp())
                )

                await fire_event("alpha_signal", alpha_signal.dict())
                return alpha_signal

            except Exception as e:
                SIGNAL_ERRORS.inc()
                logger.error(f"[Nova][ALPHA][evaluate] Error: {e} | {symbol}")
                raise

    def _choose_flavor(self, scores: Dict[str, float], user_context: Optional[Dict]) -> str:
        """
        Assign flavor tag for alpha signal.
        """
        avg = sum(scores.values()) / max(len(scores), 1)
        if user_context:
            risk_profile = user_context.get("risk")
            if risk_profile == "degen": return "Degen"
            if risk_profile == "safe": return "Safe"
        if avg > 0.85: return "Safe"
        if avg < 0.3: return "Degen"
        return "Moderate"

    def _adjust_signal_based_on_market_depth(self, scores: Dict[str, float], liquidity: float, whale_pressure: float) -> float:
        """
        Adjust the signal's score based on market depth, including liquidity and whale activity.
        """
        # Market depth adjustment factor
        market_depth_factor = 1.0
        if liquidity < 0.1:
            market_depth_factor *= 0.9  # Reduce signal strength in low liquidity
        if whale_pressure > 0.8:
            market_depth_factor *= 1.2  # Increase signal strength if whales are active

        return market_depth_factor
