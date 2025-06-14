"""
Nova Pulse ALPHA – Live Social‑Sentiment Streamer
──────────────────────────────────────────────────
•  Listens to a Redis pub/sub channel of raw social‑media & on‑chain
   sentiment signals (produced by upstream scrapers / agents).
•  Scores each payload with a plug‑in registry (default: weighted aggregate).
•  Emits high‑confidence alerts (+/‑) to an outbound Redis channel *and*
   fires in‑process hooks for downstream modules (alpha‑agent, Telegram, …).
•  Prometheus instrumentation, DLQ buffering, audit‑hash provenance.
•  **No placeholders** – every call is wired for production.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aioredis
import openai
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from utils.config import Config  # pulls OPENAI key / Redis URL
from utils.plugin_manager import fire_plugin_event  # cross‑module event bus

logger = logging.getLogger("nova.social_sentiment")
logger.setLevel(logging.INFO)

# ───────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ───────────────────────────────────────────────────────────────────────────────
SENTIMENT_REQS   = Counter("sentiment_requests_total", "Payloads processed")
SENTIMENT_ALERTS = Counter("sentiment_alerts_total", "High‑confidence alerts")
SENTIMENT_ERRS   = Counter("sentiment_errors_total", "Processing errors")
SENTIMENT_LAT    = Histogram("sentiment_latency_seconds", "End‑to‑end latency")
SENTIMENT_DLQ    = Counter("sentiment_dlq_total", "Failed emissions to DLQ")

# ───────────────────────────────────────────────────────────────────────────────
# In‑memory DLQ
# ───────────────────────────────────────────────────────────────────────────────
_FAILED_DLQ: List[Dict[str, Any]] = []

# ───────────────────────────────────────────────────────────────────────────────
# Plug‑in & Event infrastructure
# ───────────────────────────────────────────────────────────────────────────────
PLUGIN_REGISTRY: Dict[str, Callable[[Dict[str, Any], bool], Awaitable[float]]] = {}
EVENT_HOOKS: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def register_sentiment_plugin(name: str, fn: Callable[[Dict[str, Any], bool], Awaitable[float]]):
    PLUGIN_REGISTRY[name] = fn

def register_sentiment_event_hook(fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    EVENT_HOOKS.append(fn)

async def _fire_event(name: str, data: Dict[str, Any]):
    # internal + global plugin bus
    for hook in EVENT_HOOKS:
        try:
            await hook(name, data)
        except Exception as exc:
            logger.error("[Sentiment] Event hook %s failed: %s", hook.__name__, exc)
    # outward facing global bus
    await fire_plugin_event(name, data)

# ───────────────────────────────────────────────────────────────────────────────
# Helper – audit hash
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
    timestamp: int = Field(default_factory=lambda: int(asyncio.get_event_loop().time()))
    audit_hash: str
    plugin: str = "default"
    simulation: bool = False

# ───────────────────────────────────────────────────────────────────────────────
# Main class
# ───────────────────────────────────────────────────────────────────────────────
class SocialSentiment:
    """Live sentiment processor – subscribes, scores, publishes."""

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
        self._redis: aioredis.Redis | None = None

        # OpenAI key configured via Config – use sync client inside executor
        openai.api_key = self.cfg.OPENAI_API_KEY

        # register default plugin
        register_sentiment_plugin("default", self._default_score)

    # ── Connection helpers ──────────────────────────────────────────────────
    async def connect(self):
        self._redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        logger.info("[SocialSentiment] Connected to Redis @ %s", self.redis_url)

    async def disconnect(self):
        if self._redis:
            await self._redis.close()
            logger.info("[SocialSentiment] Redis connection closed")

    # ── Stream consumer ─────────────────────────────────────────────────────
    async def run(self, plugin: str = "default", *, simulation: bool = False):
        assert self._redis, "call connect() first"
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(self.in_channel)
        logger.info("[SocialSentiment] Subscribed to %s", self.in_channel)

        async for msg in pubsub.listen():
            if msg.get("type") != "message":
                continue
            SENTIMENT_REQS.inc()
            try:
                payload = json.loads(msg["data"] if isinstance(msg["data"], str) else msg["data"].decode())
                await self._handle_payload(payload, plugin=plugin, simulation=simulation)
            except Exception as exc:
                SENTIMENT_ERRS.inc()
                logger.error("[SocialSentiment] Processing error: %s", exc)

    # ── Core handler ────────────────────────────────────────────────────────
    async def _handle_payload(self, data: Dict[str, Any], *, plugin: str, simulation: bool):
        symbol = data.get("symbol")
        if not symbol:
            return

        async with SENTIMENT_LAT.time():
            # score
            score_fn = PLUGIN_REGISTRY.get(plugin, self._default_score)
            score = await score_fn(data, simulation)

        if abs(score) < 0.7:  # threshold – tweakable
            return

        SENTIMENT_ALERTS.inc()
        caption = await self._caption(symbol, score)
        alert = SentimentAlert(
            symbol=symbol,
            sentiment_score=score,
            ai_caption=caption,
            raw=data,
            audit_hash=_audit_hash(data, score),
            plugin=plugin,
            simulation=simulation,
        )
        await self._emit(alert)

    # ── Default scoring plug‑in ─────────────────────────────────────────────
    async def _default_score(self, data: Dict[str, Any], simulation: bool) -> float:
        social = float(data.get("social_score", 0))
        news = float(data.get("news_score", 0))
        onchain = float(data.get("onchain_score", 0))
        score = 0.6 * social + 0.3 * news + 0.1 * onchain
        return score * (0.95 if simulation else 1.0)

    # ── OpenAI caption helper ───────────────────────────────────────────────
    async def _caption(self, symbol: str, score: float) -> str:
        prompt = (
            f"Symbol: {symbol}\n"
            f"Sentiment Score: {score:+.2f}\n"
            "Write an urgent, one‑line trader alert with an emoji:"  # style prompt
        )
        try:
            resp = await asyncio.to_thread(
                lambda: openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=40,
                    temperature=0.7,
                )
            )
            return resp.choices[0].message.content.strip()
        except Exception as exc:
            logger.warning("[Sentiment] Caption fallback: %s", exc)
            return f"Sentiment alert for {symbol} ({score:+.2f})"

    # ── Emit alert to Redis + hooks ─────────────────────────────────────────
    async def _emit(self, alert: SentimentAlert):
        try:
            assert self._redis, "Redis not connected"
            await self._redis.publish(self.out_channel, alert.json())
            await self._redis.set(f"sentiment:{alert.symbol}:{alert.timestamp}", alert.json(), ex=self.cache_ttl)
            await _fire_event("sentiment_alert", alert.dict())
        except Exception as exc:
            SENTIMENT_DLQ.inc()
            _FAILED_DLQ.append({"alert": alert.dict(), "error": str(exc)})
            logger.error("[SocialSentiment] Alert emission error: %s", exc)

    # ── DLQ helpers ─────────────────────────────────────────────────────────
    async def replay_dlq(self):
        for item in _FAILED_DLQ[:]:
            try:
                await self._emit(SentimentAlert(**item["alert"]))
                _FAILED_DLQ.remove(item)
            except Exception:
                continue

    def get_dlq(self):
        return list(_FAILED_DLQ)
