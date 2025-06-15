# core/signal_generation/signal_sources.py

import asyncio
import hashlib
import json
import logging
import time
import uuid
from typing import Any, Dict, Optional, List

from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Counter, Histogram

from core.adaptive_tp_sl import AdaptiveTPSL
from core.data_collection.orderbook_overlay import OrderBookOverlay
from core.utils.config import config

logger = logging.getLogger("nova.signal_sources")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
SOURCE_FETCHES    = Counter("signal_source_fetches_total",      "Signal source fetches")
SOURCE_ERRORS     = Counter("signal_source_errors_total",       "Signal source errors")
SOURCE_LATENCY    = Histogram("signal_source_latency_seconds",   "Signal fetch latency")
SOURCE_DLQ        = Counter("signal_source_dlq_total",         "Signal source DLQ events")
SOURCE_ALERTS     = Counter("signal_source_alert_total",        "Signal source alert triggers")
PLUGIN_LATENCY    = Histogram("signal_source_plugin_latency_seconds", "Latency per plugin", ["plugin"])
PLUGIN_ERRORS     = Counter("signal_source_plugin_errors_total",   "Errors per plugin",   ["plugin"])

# ────────────────────────────────────────────────────────────────────────────────
# Plugin Registry & DLQ
# ────────────────────────────────────────────────────────────────────────────────
FAILED_SOURCE_DLQ  : List[Dict[str, Any]] = []
source_event_hooks   : List[Any] = []
source_plugins       : Dict[str, Any] = {}
_cache_locks         : Dict[str, asyncio.Lock] = {}


def register_source_event_hook(fn: Any):
    source_event_hooks.append(fn)

async def fire_source_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in source_event_hooks:
        try:
            await hook(event, payload)
        except Exception:
            logger.warning(f"[SignalSources] Event hook {hook.__name__} failed")


def register_source_plugin(name: str, fn: Any) -> None:
    source_plugins[name] = fn

# ────────────────────────────────────────────────────────────────────────────────
# Data Models
# ────────────────────────────────────────────────────────────────────────────────
class RawSignal(BaseModel):
    price: float = Field(..., gt=0)
    volume: float = Field(0)
    trend: float = Field(0)
    sentiment: float = Field(0)
    news: float = Field(0)
    alert: bool = Field(False)
    timestamp: int
    simulation: bool

# ────────────────────────────────────────────────────────────────────────────────
# SignalSources Class
# ────────────────────────────────────────────────────────────────────────────────
class SignalSources:
    def __init__(self,
                 adaptive_tp_sl: AdaptiveTPSL,
                 orderbook_overlay: OrderBookOverlay,
                 cache_ttl: int = 5
                 ):
        self.adaptive_tp_sl = adaptive_tp_sl
        self.orderbook_overlay = orderbook_overlay
        self.cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = cache_ttl

    async def fetch_source(self,
                           ctx: Dict[str, Any],
                           plugin: str = "default",
                           user: Optional[str] = None,
                           simulation: bool = False
                           ) -> Dict[str, Any]:
        SOURCE_FETCHES.inc()
        start = time.time()
        trace_id = ctx.get("trace_id") or f"sig-{uuid.uuid4().hex[:8]}"
        ctx["trace_id"] = trace_id
        key = hashlib.sha256(json.dumps(ctx, sort_keys=True).encode()).hexdigest()

        # Stampede-safe cache
        lock = _cache_locks.setdefault(key, asyncio.Lock())
        async with lock:
            entry = self.cache.get(key)
            if entry and time.time() - entry["ts"] < self.cache_ttl:
                result = entry["value"]
                SOURCE_LATENCY.observe(time.time() - start)
                return result

            try:
                # Parallel plugin fetch
                if plugin == "ensemble":
                    raw = await self._fetch_ensemble(ctx, user, simulation)
                else:
                    fn = source_plugins.get(plugin, self.default_source)
                    raw = await asyncio.wait_for(fn(ctx, user=user, simulation=simulation), timeout=3.0)

                # Validate and normalize
                try:
                    signal = RawSignal(**raw)
                except ValidationError as ve:
                    raise RuntimeError(f"Plugin output invalid: {ve}")

                # Enrich with TP/SL (isolated)
                try:
                    tp_sl = await self.adaptive_tp_sl.compute(ctx.get("symbol"), signal.price, signal.dict())
                    signal_dict = signal.dict()
                    signal_dict.update(tp_sl)
                except Exception as e:
                    logger.warning(f"[SignalSources] TP/SL enrichment failed: {e}")
                    signal_dict = signal.dict()

                # Enrich with orderbook data (isolated)
                try:
                    ob = await self.orderbook_overlay.analyze(ctx.get("symbol"))
                    signal_dict["orderbook_data"] = ob
                except Exception as e:
                    logger.warning(f"[SignalSources] Orderbook enrichment failed: {e}")

                # Build audit
                audit = {
                    "ctx": ctx,
                    "source": signal_dict,
                    "user": user,
                    "simulation": simulation,
                    "trace_id": trace_id,
                    "timestamp": int(time.time()),
                    "audit_hash": hashlib.sha256(json.dumps(signal_dict, sort_keys=True).encode()).hexdigest()
                }

                # Cache and emit events
                self.cache[key] = {"ts": time.time(), "value": audit}
                await fire_source_event("source_fetched", audit)

                if signal.alert:
                    SOURCE_ALERTS.inc()
                    await fire_source_event("source_alert", audit)

                return audit

            except Exception as e:
                SOURCE_ERRORS.inc()
                SOURCE_DLQ.inc()
                err = {"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id}
                FAILED_SOURCE_DLQ.append(err)
                await fire_source_event("source_error", err)
                logger.error(f"[SignalSources] fetch_source error: {e}")
                return err

            finally:
                SOURCE_LATENCY.observe(time.time() - start)

    async def _fetch_ensemble(self,
                              ctx: Dict[str, Any],
                              user: Optional[str],
                              simulation: bool
                              ) -> Dict[str, Any]:
        tasks = []
        for name, fn in source_plugins.items():
            tasks.append(self._run_plugin(name, fn, ctx, user, simulation))

        results = await asyncio.gather(*tasks)
        # Filter successes
        good = [r for r in results if isinstance(r, dict)]
        if not good:
            return await self.default_source(ctx, user, simulation)

        # Aggregate
        agg = {
            "price": sum(r["price"] for r in good) / len(good),
            "volume": sum(r.get("volume", 0) for r in good) / len(good),
            "trend": sum(r.get("trend", 0) for r in good) / len(good),
            "sentiment": sum(r.get("sentiment", 0) for r in good) / len(good),
            "news": sum(r.get("news", 0) for r in good) / len(good),
            "alert": any(r.get("alert") for r in good),
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation
        }
        return agg

    async def _run_plugin(self,
                          name: str,
                          fn: Any,
                          ctx: Dict[str, Any],
                          user: Optional[str],
                          simulation: bool
                          ) -> Any:
        start = time.time()
        try:
            result = await asyncio.wait_for(fn(ctx, user=user, simulation=simulation), timeout=3.0)
            PLUGIN_LATENCY.labels(plugin=name).observe(time.time() - start)
            return result
        except Exception as e:
            PLUGIN_ERRORS.labels(plugin=name).inc()
            logger.warning(f"[SignalSources] plugin {name} failed: {e}")
            return {}

    async def replay_dlq(self) -> None:
        for entry in FAILED_SOURCE_DLQ[:]:
            try:
                await self.fetch_source(entry.get("ctx", {}), user=entry.get("user"))
                FAILED_SOURCE_DLQ.remove(entry)
            except: pass

    def get_dlq(self) -> List[Dict[str, Any]]:
        return list(FAILED_SOURCE_DLQ)

    def get_last(self) -> Optional[Dict[str, Any]]:
        return self.cache.get(next(iter(self.cache)), {}).get("value")
