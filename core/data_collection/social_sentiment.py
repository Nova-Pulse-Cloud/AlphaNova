"""
Nova Pulse ALPHA – Live Social-Sentiment Streamer (v2.1)
────────────────────────────────────────────────────────
• Async, no blocking; direct Redis pub/sub from live Nova scrapers/agents
• Plug-in scoring + event system
• OpenAI caption fallback; bulletproof Prometheus metrics, audit hash
• No placeholders; 100% production-grade JSON handling
• Compatible with `redis.asyncio` (not aioredis)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional

import redis.asyncio as redis_async  # Modern Redis (no aioredis)
import openai
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from utils.config import Config
from utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.social_sentiment")
logger.setLevel(logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ───────────────────────────────────────────────────────────────────────────────
SENTIMENT_PAYLOADS = Counter("sentiment_payloads_total", "Payloads processed")
SENTIMENT_ALERTS  = Counter("sentiment_alerts_total", "High-confidence alerts emitted")
SENTIMENT_ERRORS  = Counter("sentiment_errors_total", "Processing errors")
SENTIMENT_LATENCY = Histogram("sentiment_latency_seconds", "End-to-end latency")
SENTIMENT_CAP_LAT = Histogram("sentiment_caption_latency_seconds", "OpenAI caption latency")
SENTIMENT_DLQ     = Counter("sentiment_dlq_total", "Emission failures (DLQ)")

# ───────────────────────────────────────────────────────────────────────────────
# In-memory DLQ (kept small – persisted elsewhere by DLQ manager)
# ───────────────────────────────────────────────────────────────────────────────
_DLQ: List[Dict[str, Any]] = []

# ───────────────────────────────────────────────────────────────────────────────
# Plug-in & Event infrastructure
# ───────────────────────────────────────────────────────────────────────────────
PLUGIN_REGISTRY: Dict[str, Callable[[Dict[str, Any], bool], Awaitable[float]]] = {}
EVENT_HOOKS: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def register_sentiment_plugin(name: str, fn: Callable[[Dict[str, Any], bool], Awaitable[float]]):
    PLUGIN_REGISTRY[name] = fn

def register_sentiment_event_hook(fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    EVENT_HOOKS.append(fn)

async def _fire_event(name: str, data: Dict[str, Any]):
    for hook in EVENT_HOOKS:
        try:
            await hook(name, data)
        except Exception as exc:
            logger.error("[Sentiment] Local event hook %s failed: %s", getattr(hook, "__name__", "?"), exc)
    await fire_plugin_event(name, data)

# ───────────────────────────────────────────────────────────────────────────────
# Helper – audit hash (SHA-256)
# ───────────────────────────────────────────────────────────────────────────────
def _audit_hash(ctx: Dict[str, Any], score: float) -> str:
    return hashlib.sha256(json.dumps({**ctx, "score": score}, sort_keys=True).encode()).hexdigest()

# ───────────────────────────────────────────────────────────────────────────────
# Pydantic DTO
# ───────────────────────────────────────────────────────────────────────────────
class SentimentAlert(BaseModel):
    symbol: str
    sentiment_score: float
    ai_caption: str
    raw: Dict[str, Any]
    timestamp: int = Field(default_factory=lambda: int(time.time()))
    audit_hash: str
    plugin: str = "default"
    simulation: bool = False

# ───────────────────────────────────────────────────────────────────────────────
# Main class
# ───────────────────────────────────────────────────────────────────────────────
class SocialSentiment:
    """Live sentiment processor – subscribes, scores, publishes."""

    # High-confidence threshold | allow override via env
    SCORE_THRESHOLD = float(Config()._get_env("SENTIMENT_ALERT_THRESHOLD", "0.7"))

    def __init__(
        self,
        cfg: Optional[Config] = None,
        *,
        in_channel: str = "sentiment_signals",
        out_channel: str = "sentiment_alerts",
        cache_ttl: int = 900,
    ) -> None:
        self.cfg = cfg or Config()
        self.redis_url = self.cfg.REDIS_URL
        self.in_channel = in_channel
        self.out_channel = out_channel
        self.cache_ttl = cache_ttl
        self._redis: Optional[redis_async.Redis] = None
        openai.api_key = self.cfg.OPENAI_API_KEY
        register_sentiment_plugin("default", self._default_score)

    async def connect(self):
        self._redis = await redis_async.from_url(self.redis_url, decode_responses=True)
        logger.info("[SocialSentiment] ✅ Connected to Redis @ %s", self.redis_url)

    async def disconnect(self):
        if self._redis:
            await self._redis.close()
            logger.info("[SocialSentiment] 🔌 Redis connection closed")

    async def run(self, plugin: str = "default", *, simulation: bool = False):
        assert self._redis, "call connect() first"
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(self.in_channel)
        logger.info("[SocialSentiment] Listening on %s", self.in_channel)
        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            SENTIMENT_PAYLOADS.inc()
            try:
                payload = json.loads(msg["data"])
                await self._handle_payload(payload, plugin=plugin, simulation=simulation)
            except Exception as exc:
                SENTIMENT_ERRORS.inc()
                logger.error("[SocialSentiment] Processing error: %s", exc)

    async def _handle_payload(self, data: Dict[str, Any], *, plugin: str, simulation: bool):
        symbol = data.get("symbol")
        if not symbol:
            return
        score_fn = PLUGIN_REGISTRY.get(plugin, self._default_score)
        async with SENTIMENT_LATENCY.time():
            score = await score_fn(data, simulation)
        if abs(score) < self.SCORE_THRESHOLD:
            return
        SENTIMENT_ALERTS.inc()
        caption = await self._caption(symbol, score)
        alert = SentimentAlert(
            symbol=symbol,
            sentiment_score=round(score, 3),
            ai_caption=caption,
            raw=data,
            audit_hash=_audit_hash(data, score),
            plugin=plugin,
            simulation=simulation,
        )
        await self._emit(alert)

    async def _default_score(self, data: Dict[str, Any], simulation: bool) -> float:
        social = float(data.get("social_score", 0))
        news = float(data.get("news_score", 0))
        onchain = float(data.get("onchain_score", 0))
        aggregate = 0.6 * social + 0.3 * news + 0.1 * onchain
        return aggregate * (0.97 if simulation else 1.0)

    async def _caption(self, symbol: str, score: float) -> str:
        prompt = (
            f"Symbol: {symbol}\nSentiment Score: {score:+.2f}\n"
            "Craft ONE punch‑y line for traders (include emoji):"
        )
        try:
            async with SENTIMENT_CAP_LAT.time():
                resp = await asyncio.to_thread(
                    lambda: openai.ChatCompletion.create(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=40,
                        temperature=0.65,
                    )
                )
            return resp.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning("[Sentiment] Caption fallback: %s", exc)
            return f"Sentiment alert for {symbol} ({score:+.2f})"

    async def _emit(self, alert: SentimentAlert):
        try:
            assert self._redis, "Redis not connected"
            await self._redis.publish(self.out_channel, alert.json())
            await self._redis.set(f"sentiment:{alert.symbol}:{alert.timestamp}", alert.json(), ex=self.cache_ttl)
            await _fire_event("sentiment_alert", alert.dict())
        except Exception as exc:
            SENTIMENT_DLQ.inc()
            _DLQ.append({"alert": alert.dict(), "error": str(exc)})
            logger.error("[SocialSentiment] ⚠️  Alert emission failed: %s", exc)

    async def replay_dlq(self):
        for item in _DLQ[:]:
            try:
                await self._emit(SentimentAlert(**item["alert"]))
                _DLQ.remove(item)
            except Exception:
                continue

    def get_dlq(self):
        return list(_DLQ)
