import asyncio
import logging
import hashlib
import json
import uuid
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram

logger = logging.getLogger("sentiment_agent")

# === Prometheus metrics ===
SENTIMENT_CALLS = Counter("sentiment_calls_total", "Sentiment API calls")
SENTIMENT_ERRORS = Counter("sentiment_errors_total", "Sentiment errors")
SENTIMENT_LATENCY = Histogram("sentiment_latency_seconds", "Sentiment latency")
SENTIMENT_DLQ = Counter("sentiment_dlq_total", "Sentiment DLQ events")
SENTIMENT_ALERTS = Counter("sentiment_alert_total", "Sentiment alert triggers")

# === DLQ, event hooks, plugin registry ===
FAILED_SENTIMENT_DLQ = []
sentiment_event_hooks = []
def register_sentiment_event_hook(hook_fn):
    sentiment_event_hooks.append(hook_fn)

async def fire_sentiment_event(event_name, data):
    for hook in sentiment_event_hooks:
        await hook(event_name, data)

sentiment_plugins = {}
def register_sentiment_plugin(name: str, fn: Callable):
    sentiment_plugins[name] = fn

# === Audit hash ===
def sentiment_hash(ctx: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class SentimentAgent:
    def __init__(self):
        self.last_sentiment = None
        self.alerts = []
        self.loop = asyncio.get_event_loop()

    async def fetch_sentiment(self, ctx: Dict[str, Any], plugin: str = "default", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        SENTIMENT_CALLS.inc()
        trace_id = f"sent-{uuid.uuid4().hex[:8]}"
        try:
            async with SENTIMENT_LATENCY.time():
                # Plugin-based multi-source sentiment fetch (Twitter, X, news, on-chain, etc)
                fn = sentiment_plugins.get(plugin, self.default_sentiment)
                sentiment = await fn(ctx, user=user, simulation=simulation)
                rationale = sentiment.get("note", "default rationale")
                audit = {
                    "ctx": ctx,
                    "sentiment": sentiment,
                    "user": user,
                    "simulation": simulation,
                    "trace_id": trace_id,
                    "rationale": rationale
                }
                audit["audit_hash"] = sentiment_hash(ctx, sentiment)
                await fire_sentiment_event("sentiment_fetched", audit)
                self.last_sentiment = audit
                if sentiment.get("alert"):
                    SENTIMENT_ALERTS.inc()
                    self.alerts.append(audit)
                    await fire_sentiment_event("sentiment_alert", audit)
                return audit
        except Exception as e:
            SENTIMENT_ERRORS.inc()
            SENTIMENT_DLQ.inc()
            FAILED_SENTIMENT_DLQ.append({"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id})
            await fire_sentiment_event("sentiment_error", {"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id})
            logger.error(f"[SentimentAgent] Sentiment fetch error: {e}")
            return {"error": str(e), "user": user, "trace_id": trace_id}

    async def default_sentiment(self, ctx: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        # Example: Combine scores from multiple sources (stub)
        price = ctx.get("price", 0.0)
        social = ctx.get("social_score", 0.0)
        news = ctx.get("news_score", 0.0)
        onchain = ctx.get("onchain_score", 0.0)
        aggregate = (0.4 * social) + (0.3 * news) + (0.2 * price) + (0.1 * onchain)
        alert = aggregate < -0.6 or aggregate > 0.75
        note = "Bullish" if aggregate > 0.5 else "Bearish" if aggregate < -0.5 else "Neutral"
        return {
            "sentiment_score": aggregate,
            "alert": alert,
            "note": note,
            "sources": {"price": price, "social": social, "news": news, "onchain": onchain},
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation,
        }

    async def replay_dlq(self):
        for failed in FAILED_SENTIMENT_DLQ[:]:
            try:
                _ = await self.fetch_sentiment(failed.get("ctx", {}), user=failed.get("user"))
                FAILED_SENTIMENT_DLQ.remove(failed)
            except Exception as e:
                continue

    def get_dlq(self):
        return FAILED_SENTIMENT_DLQ.copy()

    def get_alerts(self):
        return self.alerts

    def get_last_sentiment(self):
        return self.last_sentiment
