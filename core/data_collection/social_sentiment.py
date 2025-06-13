# core/social_sentiment.py (merged: operational + pro, async, mesh, plugin, metrics, DLQ, audit, ops, simulation, RL)
import asyncio
import logging
import aioredis
import openai
import hashlib
import json
from typing import Dict, Any, Optional, Callable, List
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

logger = logging.getLogger("social_sentiment")

# === Prometheus metrics ===
SENTIMENT_SCAN_REQUESTS = Counter("sentiment_scan_requests_total", "Total social sentiment scans")
SENTIMENT_ALERTS = Counter("sentiment_alerts_total", "Total sentiment alerts detected")
SENTIMENT_ERRORS = Counter("sentiment_errors_total", "Social sentiment errors")
SENTIMENT_LATENCY = Histogram("sentiment_latency_seconds", "Latency of sentiment computations")
SENTIMENT_DLQ = Counter("sentiment_dlq_total", "Social sentiment DLQ events")

# === DLQ, plugin/event system ===
FAILED_SENTIMENT_DLQ = []
sentiment_event_hooks = []
sentiment_plugins = {}

def register_sentiment_plugin(name: str, fn: Callable):
    sentiment_plugins[name] = fn

def register_sentiment_event_hook(hook_fn):
    sentiment_event_hooks.append(hook_fn)

async def fire_sentiment_event(event_name, data):
    for hook in sentiment_event_hooks:
        await hook(event_name, data)

def sentiment_hash(ctx: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class SentimentAlert(BaseModel):
    symbol: str
    sentiment_score: float
    ai_caption: str
    details: Dict[str, Any]
    timestamp: int
    audit_hash: str
    plugin: str = "default"
    simulation: bool = False

class SocialSentiment:
    def __init__(
        self,
        redis_url: str,
        openai_api_key: str,
        sentiment_channel: str = "sentiment_signals",
        output_channel: str = "sentiment_alerts",
        cache_ttl: int = 900,
        plugins: Optional[Dict[str, Callable]] = None,
    ):
        self.redis_url = redis_url
        self.sentiment_channel = sentiment_channel
        self.output_channel = output_channel
        self.cache_ttl = cache_ttl
        self.redis = None
        openai.api_key = openai_api_key
        self.plugins = plugins or {}
        register_sentiment_plugin("default", self._sentiment_score)

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url)
        logger.info("SocialSentiment connected to Redis.")

    async def disconnect(self):
        await self.redis.close()
        logger.info("SocialSentiment disconnected.")

    async def process_stream(self, plugin: str = "default", simulation: bool = False):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.sentiment_channel)
        logger.info("SocialSentiment subscribed to channel.")
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            SENTIMENT_SCAN_REQUESTS.inc()
            try:
                data = self._parse_message(message)
                if data:
                    await self.handle_signal(data, plugin=plugin, simulation=simulation)
            except Exception as e:
                SENTIMENT_ERRORS.inc()
                logger.error(f"Sentiment processing error: {e}")

    def _parse_message(self, message):
        try:
            channel = message.get("channel")
            if not isinstance(channel, str):
                channel = channel.decode()
            payload = message["data"]
            if isinstance(payload, bytes):
                payload = payload.decode()
            data = eval(payload) if isinstance(payload, str) else payload
            data["channel"] = channel
            return data
        except Exception:
            return None

    async def handle_signal(self, data: Dict[str, Any], plugin: str = "default", simulation: bool = False):
        symbol = data.get("symbol")
        if not symbol:
            return

        with SENTIMENT_LATENCY.time():
            # Use plugin registry for custom scoring
            plugin_fn = sentiment_plugins.get(plugin, self._sentiment_score)
            sentiment_score = await plugin_fn(data, simulation=simulation)

            if abs(sentiment_score) > 0.7:
                SENTIMENT_ALERTS.inc()
                ai_caption = await self._ai_caption(symbol, sentiment_score)
                alert = SentimentAlert(
                    symbol=symbol,
                    sentiment_score=sentiment_score,
                    ai_caption=ai_caption,
                    details=data,
                    timestamp=int(asyncio.get_event_loop().time()),
                    audit_hash=sentiment_hash(data, {"sentiment_score": sentiment_score}),
                    plugin=plugin,
                    simulation=simulation,
                )
                try:
                    await self._emit_alert(alert)
                    await fire_sentiment_event("sentiment_alert", alert.dict())
                except Exception as e:
                    SENTIMENT_DLQ.inc()
                    FAILED_SENTIMENT_DLQ.append({"data": data, "error": str(e)})
                    logger.error(f"Sentiment alert emission failed: {e}")

    async def _sentiment_score(self, data: Dict[str, Any], simulation: bool = False):
        # Example: Use social, news, and on-chain scores if present
        social = float(data.get("social_score", 0.0))
        news = float(data.get("news_score", 0.0))
        onchain = float(data.get("onchain_score", 0.0))
        aggregate = (0.6 * social) + (0.3 * news) + (0.1 * onchain)
        if simulation:
            aggregate *= 0.95
        return max(-1.0, min(aggregate, 1.0))

    async def _ai_caption(self, symbol, sentiment_score):
        prompt = (
            f"Symbol: {symbol}\n"
            f"Sentiment Score: {sentiment_score:.2f}\n"
            "Write a one-sentence alert for traders about this sentiment signal:"
        )
        try:
            response = await asyncio.to_thread(
                lambda: openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}],
                    max_tokens=50,
                    temperature=0.7,
                )
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"AI caption error: {e}")
            return f"Sentiment alert: {symbol}, score {sentiment_score:.2f}"

    async def _emit_alert(self, alert: SentimentAlert):
        await self.redis.publish(self.output_channel, alert.json())
        await self.redis.set(
            f"sentiment:{alert.symbol}:{alert.timestamp}",
            alert.json(),
            ex=self.cache_ttl,
        )
        logger.info(f"Sentiment alert emitted for {alert.symbol}")

    async def replay_dlq(self):
        for failed in FAILED_SENTIMENT_DLQ[:]:
            try:
                await self.handle_signal(failed.get("data", {}))
                FAILED_SENTIMENT_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_SENTIMENT_DLQ.copy()
