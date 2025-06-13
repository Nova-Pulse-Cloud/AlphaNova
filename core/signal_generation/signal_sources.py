import asyncio
import logging
import hashlib
import json
import uuid
import time
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram
from core.adaptive_tp_sl import AdaptiveTPSL  # Import AdaptiveTPSL to integrate dynamic TP/SL
from core.orderbook_overlay import OrderBookOverlay  # Import OrderBookOverlay for market depth analysis

logger = logging.getLogger("signal_sources")

# === Prometheus metrics ===
SOURCE_FETCHES = Counter("signal_source_fetches_total", "Signal source fetches")
SOURCE_ERRORS = Counter("signal_source_errors_total", "Signal source errors")
SOURCE_LATENCY = Histogram("signal_source_latency_seconds", "Signal source latency")
SOURCE_DLQ = Counter("signal_source_dlq_total", "Signal source DLQ events")
SOURCE_ALERTS = Counter("signal_source_alert_total", "Signal source alert triggers")

# === DLQ, event hooks, plugin registry ===
FAILED_SOURCE_DLQ = []
source_event_hooks = []

def register_source_event_hook(hook_fn):
    source_event_hooks.append(hook_fn)

async def fire_source_event(event_name, data):
    for hook in source_event_hooks:
        await hook(event_name, data)

source_plugins = {}

def register_source_plugin(name: str, fn: Callable):
    source_plugins[name] = fn

# === Audit hash ===
def source_hash(source: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(source, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class SignalSources:
    def __init__(self, adaptive_tp_sl: AdaptiveTPSL, orderbook_overlay: OrderBookOverlay):
        self.last_source = None
        self.alerts = []
        self.loop = asyncio.get_event_loop()
        self.cache = {}
        self.cache_ttl = 5  # seconds
        self.adaptive_tp_sl = adaptive_tp_sl  # Initialize the adaptive TP/SL module
        self.orderbook_overlay = orderbook_overlay  # Initialize the OrderBookOverlay module

    async def fetch_source(self, ctx: Dict[str, Any], plugin: str = "default", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        SOURCE_FETCHES.inc()
        trace_id = ctx.get("trace_id") or f"sig-{uuid.uuid4().hex[:8]}"
        ctx["trace_id"] = trace_id
        key = json.dumps(ctx, sort_keys=True)

        # Check cache
        if key in self.cache:
            if time.time() - self.cache[key]["ts"] < self.cache_ttl:
                return self.cache[key]["value"]

        try:
            async with SOURCE_LATENCY.time():
                # Fetch signals based on the plugin type
                if plugin == "ensemble":
                    result = await self._fetch_ensemble(ctx, user, simulation)
                else:
                    fn = source_plugins.get(plugin, self.default_source)
                    result = await asyncio.wait_for(fn(ctx, user=user, simulation=simulation), timeout=4.0)

                # Integrate the signal data into adaptive TP/SL system
                signal_data = await self._process_for_tp_sl(ctx, result)

                # Integrate OrderBookOverlay data for liquidity and market depth analysis
                orderbook_data = await self.orderbook_overlay.analyze(ctx.get("symbol"))
                signal_data["orderbook_data"] = orderbook_data  # Attach market depth data to the signal

                result["trace_id"] = trace_id
                rationale = ctx.get("note", result.get("note", "No rationale"))
                audit = {
                    "ctx": ctx,
                    "source": result,
                    "user": user,
                    "simulation": simulation,
                    "trace_id": trace_id,
                    "rationale": rationale,
                    "audit_hash": source_hash(ctx, result)
                }

                # Fire events for fetched signals
                await fire_source_event("source_fetched", audit)
                self.last_source = audit

                if result.get("alert"):
                    SOURCE_ALERTS.inc()
                    self.alerts.append(audit)
                    await fire_source_event("source_alert", audit)

                self.cache[key] = {"ts": time.time(), "value": audit}
                return audit

        except Exception as e:
            SOURCE_ERRORS.inc()
            SOURCE_DLQ.inc()
            err_data = {"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id}
            FAILED_SOURCE_DLQ.append(err_data)
            await fire_source_event("source_error", err_data)
            logger.error(f"[SignalSources] Source fetch error: {e}")
            return {"error": str(e), "user": user, "trace_id": trace_id}

    async def _fetch_ensemble(self, ctx: Dict[str, Any], user: Optional[str], simulation: bool) -> Dict[str, Any]:
        responses = []
        for name, plugin in source_plugins.items():
            try:
                result = await asyncio.wait_for(plugin(ctx, user=user, simulation=simulation), timeout=3.5)
                responses.append(result)
            except Exception as e:
                logger.warning(f"[SignalSources] Plugin {name} failed: {e}")
                continue

        if not responses:
            return await self.default_source(ctx, user, simulation)

        # Aggregating results from multiple plugins
        agg = {
            "price": sum(r.get("price", 0) for r in responses) / len(responses),
            "volume": sum(r.get("volume", 0) for r in responses) / len(responses),
            "trend": sum(r.get("trend", 0) for r in responses) / len(responses),
            "sentiment": sum(r.get("sentiment", 0) for r in responses) / len(responses),
            "news": sum(r.get("news", 0) for r in responses) / len(responses),
            "alert": any(r.get("alert") for r in responses),
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation
        }
        return agg

    async def _process_for_tp_sl(self, ctx: Dict[str, Any], result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the fetched signal data for integration into the adaptive TP/SL system.
        Uses the AdaptiveTPSL class to calculate dynamic TP/SL based on the signal.
        """
        signal_context = {
            "price": result.get("price"),
            "volatility": result.get("volatility", 0.02),
            "trend_strength": result.get("trend", 0.0),
            "whale_pressure": result.get("whale_pressure", 0.0),
            "spread": result.get("spread", 0.001),
            "gas_cost": result.get("gas_cost", 0.0),
            "orderbook_bias": result.get("orderbook_bias", 0.0),
            "rug_score": result.get("rug_score", 0.0),
            "cycle_phase": result.get("cycle_phase", 0.5),
            "user_risk": result.get("user_risk", 0.0)
        }
        # Use the AdaptiveTPSL class to compute dynamic TP/SL based on the signal
        tp_sl = await self.adaptive_tp_sl.compute(ctx.get("symbol"), result.get("price"), signal_context)
        
        # Append the TP/SL data to the result
        result["tp"] = tp_sl["tp"]
        result["sl"] = tp_sl["sl"]
        result["rr_ratio"] = tp_sl["rr_ratio"]
        result["size"] = tp_sl["size"]

        return result

    async def default_source(self, ctx: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        price = ctx.get("price", 0.0)
        volume = ctx.get("volume", 0.0)
        news = ctx.get("news", 0.0)
        trend = ctx.get("trend", 0.0)
        sentiment = ctx.get("sentiment", 0.0)
        alert = abs(price) > 1_000_000 or trend > 0.85
        return {
            "price": price,
            "volume": volume,
            "news": news,
            "trend": trend,
            "sentiment": sentiment,
            "alert": alert,
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation
        }

    async def replay_dlq(self):
        for failed in FAILED_SOURCE_DLQ[:]:
            try:
                _ = await self.fetch_source(failed.get("ctx", {}), user=failed.get("user"))
                FAILED_SOURCE_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_SOURCE_DLQ.copy()

    def get_alerts(self):
        return self.alerts

    def get_last_source(self):
        return self.last_source
