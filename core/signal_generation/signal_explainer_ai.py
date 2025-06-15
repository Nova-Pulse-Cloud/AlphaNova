# core/signal_generation/signal_explainer_ai.py
"""
Nova Pulse ALPHA – SignalExplainerAI 4.0
────────────────────────────────────────────────────────────────────
• Async, Redis-cached LLM explanations with per-signal TTL
• Adaptive TP/SL & OrderBookOverlay integration for live trade management
• Prometheus metrics (calls, latency, errors, cache hits/misses, alerts)
• DLQ for failed explainers with replay support
• Pydantic models for type safety
"""
import asyncio
import hashlib
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional

import openai
import redis.asyncio as redis_client
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from core.data_collection.orderbook_overlay import OrderBookOverlay
from core.utils.config import config

logger = logging.getLogger("nova.signal_explainer_ai")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
EXPLAIN_CALLS    = Counter("explain_calls_total",    "Total explanation requests")
EXPLAIN_ERRORS   = Counter("explain_errors_total",   "Explanation errors")
EXPLAIN_LATENCY  = Histogram("explain_latency_seconds", "Latency per explanation")
CACHE_HITS       = Counter("explain_cache_hits_total",   "Cache hits for explanations")
CACHE_MISSES     = Counter("explain_cache_misses_total", "Cache misses for explanations")
EXPLAIN_ALERTS   = Counter("explain_alerts_total",   "Explanations that triggered alerts")

# ────────────────────────────────────────────────────────────────────────────────
# DLQ
# ────────────────────────────────────────────────────────────────────────────────
FAILED_EXPLAIN_DLQ: list[Dict[str, Any]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Pydantic model for explanation output
# ────────────────────────────────────────────────────────────────────────────────
class ExplanationResult(BaseModel):
    narrative: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    alert: bool
    timestamp: int
    tp: float = Field(..., ge=0.0)
    sl: float = Field(..., ge=0.0)
    rr_ratio: float = Field(..., ge=0.0)
    size: float = Field(..., ge=0.0)
    audit_hash: str

# ────────────────────────────────────────────────────────────────────────────────
# Plugin registry (optional custom explainers)
# ────────────────────────────────────────────────────────────────────────────────
_explain_plugins: Dict[str, Any] = {}

def register_explain_plugin(name: str, fn: Any) -> None:
    _explain_plugins[name] = fn
    logger.info(f"[SignalExplainerAI] Registered plugin: {name}")

# ────────────────────────────────────────────────────────────────────────────────
# Helper: compute audit hash
# ────────────────────────────────────────────────────────────────────────────────
def _compute_hash(signal: Dict[str, Any], explanation: Dict[str, Any]) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(signal, sort_keys=True).encode())
    m.update(json.dumps(explanation, sort_keys=True).encode())
    return m.hexdigest()

# ────────────────────────────────────────────────────────────────────────────────
# Main Service
# ────────────────────────────────────────────────────────────────────────────────
class SignalExplainerAI:
    def __init__(
        self,
        ttl: int = 60,
        model: str = "gpt-4-turbo",
    ) -> None:
        self.redis_url = config.REDIS_URL
        self.cache_ttl = ttl
        self.model      = model
        self.redis: Optional[redis_client.Redis] = None
        self.orderbook = OrderBookOverlay(config)
        self.tp_sl     = AdaptiveTPSL(config)
        openai.api_key = config.OPENAI_API_KEY

    async def _connect(self):
        if not self.redis:
            try:
                self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
                logger.info("[SignalExplainerAI] Redis connected")
            except Exception as e:
                logger.error(f"[SignalExplainerAI] Redis connect failed: {e}")
                self.redis = None

    async def explain(
        self,
        signal: Dict[str, Any],
        plugin: str = "default",
        simulation: bool = False,
    ) -> ExplanationResult:
        EXPLAIN_CALLS.inc()
        start = time.time()
        trace_id = signal.get("trace_id") or f"expl-{uuid.uuid4().hex[:8]}"
        cache_key = f"explain:{trace_id}:{plugin}"

        await self._connect()

        # 1. Try cache
        if self.redis:
            try:
                raw = await self.redis.get(cache_key)
                if raw:
                    CACHE_HITS.inc()
                    data = json.loads(raw)
                    return ExplanationResult.parse_obj(data)
            except Exception:
                pass
        CACHE_MISSES.inc()

        # 2. Generate via plugin or default
        try:
            async with EXPLAIN_LATENCY.time():
                # Base explanation
                if plugin in _explain_plugins:
                    expl = await asyncio.wait_for(_explain_plugins[plugin](signal, simulation), timeout=30)
                else:
                    expl = await self._default_explain(signal, simulation)

                # Attach TP/SL & size from AdaptiveTPSL + OrderBookOverlay
                ob_data = await self.orderbook.analyze(signal.get("symbol"))
                tp_sl = await asyncio.wait_for(
                    self.tp_sl.compute(signal.get("symbol"), expl.get("price", 0.0), {**signal, **ob_data}),
                    timeout=20
                )

                full = {
                    **expl,
                    **ob_data,
                    **tp_sl,
                    "timestamp": int(time.time()),
                }
                full["audit_hash"] = _compute_hash(signal, full)
                result = ExplanationResult.parse_obj(full)

        except Exception as e:
            EXPLAIN_ERRORS.inc()
            FAILED_EXPLAIN_DLQ.append({"signal": signal, "error": str(e), "trace_id": trace_id})
            logger.error(f"[SignalExplainerAI] explain error: {e}")
            # fallback minimal explanation
            result = ExplanationResult(
                narrative="Explanation failed",
                confidence=0.0,
                alert=False,
                timestamp=int(time.time()),
                tp=signal.get("entry_price", 0.0) * 1.02,
                sl=signal.get("entry_price", 0.0) * 0.98,
                rr_ratio=1.0,
                size=0.0,
                audit_hash=_compute_hash(signal, {"error": str(e)})
            )
        finally:
            EXPLAIN_LATENCY.observe(time.time() - start)

        # 3. Cache and return
        if self.redis:
            try:
                await self.redis.set(cache_key, result.json(), ex=self.cache_ttl)
            except Exception:
                pass

        if result.alert:
            EXPLAIN_ALERTS.inc()

        return result

    async def _default_explain(self, signal: Dict[str, Any], simulation: bool) -> Dict[str, Any]:
        # Simple narrative synthesis
        trend = signal.get("trend", 0.0)
        sentiment = signal.get("sentiment", 0.0)
        narrative = f"Trend: {trend:.2f}, Sentiment: {sentiment:.2f}"
        alert = abs(trend) > 0.8 or sentiment > 0.7
        confidence = min(1.0, max(0.0, abs(trend)*0.5 + sentiment*0.5))
        return {
            "narrative": narrative,
            "confidence": confidence,
            "alert": alert,
            "price": signal.get("price", 0.0)
        }

    async def replay_dlq(self):
        """Retry all failed explanations."""
        for entry in FAILED_EXPLAIN_DLQ[:]:
            try:
                _ = await self.explain(entry["signal"])
                FAILED_EXPLAIN_DLQ.remove(entry)
            except:
                continue

    def get_dlq(self):
        return list(FAILED_EXPLAIN_DLQ)

# singleton instance
signal_explainer = SignalExplainerAI()
