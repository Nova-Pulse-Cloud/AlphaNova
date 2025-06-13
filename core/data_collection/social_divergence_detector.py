# Merged pro + operational: core/social_divergence_detector.py

import asyncio
import logging
import numpy as np
import aioredis
import httpx
import openai
import hashlib
import json
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram
from typing import Dict, Any, Callable, Optional, List

logger = logging.getLogger("social_divergence_detector")

# === Prometheus metrics ===
DIVERGENCE_SCAN_REQUESTS = Counter("divergence_scan_requests_total", "Total social divergence scans")
DIVERGENCE_FOUND = Counter("divergence_found_total", "Total divergences detected")
DIVERGENCE_ERRORS = Counter("divergence_errors_total", "Total divergence errors")
DIVERGENCE_LATENCY = Histogram("divergence_latency_seconds", "Latency of divergence computations")
DIVERGENCE_DLQ = Counter("divergence_dlq_total", "Divergence DLQ events")

# === DLQ, plugin/event system ===
FAILED_DIV_DLQ = []
div_event_hooks = []
div_plugins = {}

def register_div_plugin(name: str, fn: Callable):
    div_plugins[name] = fn

def register_div_event_hook(hook_fn):
    div_event_hooks.append(hook_fn)

async def fire_div_event(event_name, data):
    for hook in div_event_hooks:
        await hook(event_name, data)

def div_hash(ctx: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class DivergenceAlert(BaseModel):
    symbol: str
    price_change: float
    social_sentiment: float
    onchain_flow: float
    divergence_score: float
    ai_caption: str
    details: Dict[str, Any]
    timestamp: int
    audit_hash: str
    plugin: str = "default"
    simulation: bool = False

class SocialDivergenceDetector:
    def __init__(
        self,
        redis_url: str,
        openai_api_key: str,
        price_channel: str = "price_ticks",
        sentiment_channel: str = "sentiment_signals",
        onchain_channel: str = "onchain_flows",
        output_channel: str = "divergence_alerts",
        cache_ttl: int = 900,
        plugins: Optional[Dict[str, Callable]] = None,
    ):
        self.redis_url = redis_url
        self.price_channel = price_channel
        self.sentiment_channel = sentiment_channel
        self.onchain_channel = onchain_channel
        self.output_channel = output_channel
        self.cache_ttl = cache_ttl
        self.redis = None
        openai.api_key = openai_api_key
        self.plugins = plugins or {}
        # Register default plugin
        register_div_plugin("default", self._divergence_score)

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url)
        logger.info("SocialDivergenceDetector connected to Redis.")

    async def disconnect(self):
        await self.redis.close()
        logger.info("SocialDivergenceDetector disconnected.")

    async def process_stream(self, plugin: str = "default", simulation: bool = False):
        """Main loop: fuse signals, scan for divergences, emit alerts."""
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(self.price_channel, self.sentiment_channel, self.onchain_channel)
        logger.info("SocialDivergenceDetector subscribed to all channels.")
        async for message in pubsub.listen():
            if message["type"] != "message":
                continue
            DIVERGENCE_SCAN_REQUESTS.inc()
            try:
                data = self._parse_message(message)
                if data:
                    await self.handle_signals(data, plugin=plugin, simulation=simulation)
            except Exception as e:
                DIVERGENCE_ERRORS.inc()
                logger.error(f"Divergence processing error: {e}")

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

    async def handle_signals(self, data: Dict[str, Any], plugin: str = "default", simulation: bool = False):
        symbol = data.get("symbol")
        if not symbol:
            return
        latest = await self._get_latest(symbol)
        if not latest:
            return

        with DIVERGENCE_LATENCY.time():
            price_change = self._compute_price_change(latest["price_history"])
            social_score = latest["social_sentiment"]
            onchain_flow = latest["onchain_flow"]

            # Use plugin registry for divergence
            plugin_fn = div_plugins.get(plugin, self._divergence_score)
            divergence_score = await plugin_fn(price_change, social_score, onchain_flow, simulation=simulation)

            if abs(divergence_score) > 0.7:
                DIVERGENCE_FOUND.inc()
                ai_caption = await self._ai_caption(symbol, price_change, social_score, onchain_flow, divergence_score)
                alert = DivergenceAlert(
                    symbol=symbol,
                    price_change=price_change,
                    social_sentiment=social_score,
                    onchain_flow=onchain_flow,
                    divergence_score=divergence_score,
                    ai_caption=ai_caption,
                    details=latest,
                    timestamp=int(asyncio.get_event_loop().time()),
                    audit_hash=div_hash(data, {"divergence_score": divergence_score}),
                    plugin=plugin,
                    simulation=simulation,
                )
                try:
                    await self._emit_alert(alert)
                    await fire_div_event("divergence_alert", alert.dict())
                except Exception as e:
                    DIVERGENCE_DLQ.inc()
                    FAILED_DIV_DLQ.append({"data": data, "error": str(e)})
                    logger.error(f"Divergence alert emission failed: {e}")

    async def _get_latest(self, symbol: str):
        try:
            price_history = await self.redis.get(f"price_hist:{symbol}")
            sentiment = await self.redis.get(f"sentiment:{symbol}")
            onchain = await self.redis.get(f"onchain:{symbol}")

            price_history = eval(price_history.decode()) if price_history else []
            sentiment = float(sentiment.decode()) if sentiment else 0.0
            onchain = float(onchain.decode()) if onchain else 0.0

            return {
                "price_history": price_history[-24:],  # last 24 data points
                "social_sentiment": sentiment,
                "onchain_flow": onchain,
            }
        except Exception:
            return None

    def _compute_price_change(self, price_history: List[float]):
        if not price_history or len(price_history) < 2:
            return 0.0
        return (price_history[-1] - price_history[0]) / price_history[0]

    async def _divergence_score(self, price_change, social_score, onchain_flow, simulation: bool = False):
        # Default plugin: flags hard divergence, onchain amplifies effect
        divergence = 0.0
        if price_change > 0 and social_score < 0:
            divergence = (price_change - social_score)
        elif price_change < 0 and social_score > 0:
            divergence = (social_score - price_change)
        divergence *= 1 + abs(onchain_flow)
        if simulation:
            divergence *= 0.95
        return max(-1.0, min(divergence, 1.0))

    async def _ai_caption(self, symbol, price_change, social, onchain, score):
        prompt = (
            f"Symbol: {symbol}\n"
            f"Price Change: {price_change:.2%}\n"
            f"Social Sentiment: {social:.2f}\n"
            f"On-chain Flow: {onchain:.2f}\n"
            f"Divergence Score: {score:.2f}\n"
            "Write a one-sentence alert for traders about this signal:"
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
            return f"Divergence alert: {symbol}, score {score:.2f}"

    async def _emit_alert(self, alert: DivergenceAlert):
        await self.redis.publish(self.output_channel, alert.json())
        await self.redis.set(
            f"divergence:{alert.symbol}:{alert.timestamp}",
            alert.json(),
            ex=self.cache_ttl,
        )
        logger.info(f"Divergence alert emitted for {alert.symbol}")

    async def replay_dlq(self):
        for failed in FAILED_DIV_DLQ[:]:
            try:
                await self.handle_signals(failed.get("data", {}))
                FAILED_DIV_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_DIV_DLQ.copy()
