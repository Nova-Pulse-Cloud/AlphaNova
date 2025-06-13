import asyncio
import logging
import hashlib
import json
import uuid
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram
from core.adaptive_tp_sl import AdaptiveTPSL  # Import AdaptiveTPSL for real-time TP/SL adjustments

logger = logging.getLogger("signal_engine_v3")

# === Prometheus Metrics ===
SIGNAL_RUNS = Counter("signal_runs_total", "Total signal evaluations")
SIGNAL_ERRORS = Counter("signal_errors_total", "Signal errors")
SIGNAL_LATENCY = Histogram("signal_latency_seconds", "Signal evaluation latency")
SIGNAL_DLQ = Counter("signal_dlq_total", "Signal DLQ events")
SIGNAL_ALERTS = Counter("signal_alert_total", "Signal alert triggers")

# === DLQ, event hooks, plugin registry ===
FAILED_SIGNAL_DLQ = []
signal_event_hooks = []
signal_plugins = {}
ensemble_plugins: List[str] = []

# === Hook registration ===
def register_signal_event_hook(hook_fn):
    signal_event_hooks.append(hook_fn)

def register_signal_plugin(name: str, fn: Callable):
    signal_plugins[name] = fn
    if name not in ensemble_plugins:
        ensemble_plugins.append(name)

# === Audit hash ===
def signal_hash(ctx: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

# === Confidence computation ===
def compute_confidence(components: List[Dict[str, Any]]) -> float:
    scores = [c.get("signal_strength", 0) for _, c in components]
    agreement = sum(1 for s in scores if abs(s) > 0.5) / max(len(scores), 1)
    stddev = (sum((s - sum(scores)/len(scores)) ** 2 for s in scores) / len(scores)) ** 0.5 if len(scores) > 1 else 0.0
    return round(min(1.0, agreement * (1 - stddev)), 3)

# === Combine ensemble plugins ===
def combine_ensemble(components: List[Dict[str, Any]]) -> Dict[str, Any]:
    avg_strength = sum(c.get("signal_strength", 0) for _, c in components) / max(len(components), 1)
    composite = {
        "signal_strength": avg_strength,
        "confidence": compute_confidence(components),
        "components": {name: comp for name, comp in components},
        "note": "Strong Buy" if avg_strength > 0.75 else "Strong Sell" if avg_strength < -0.4 else "Hold",
        "alert": avg_strength > 0.85 or avg_strength < -0.5
    }
    return composite

class SignalEngineV3:
    def __init__(self, adaptive_tp_sl: AdaptiveTPSL):
        self.last_signal = None
        self.alerts = []
        self.loop = asyncio.get_event_loop()
        self.adaptive_tp_sl = adaptive_tp_sl  # Integrate AdaptiveTP/SL module for dynamic adjustments

    async def generate_signal(self, ctx: Dict[str, Any], plugin: str = "ensemble", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        SIGNAL_RUNS.inc()
        trace_id = ctx.get("trace_id") or f"sig-{uuid.uuid4().hex[:8]}"
        try:
            async with SIGNAL_LATENCY.time():
                # Fetch the signal based on plugin
                if plugin == "ensemble":
                    signal = await self.ensemble_signal(ctx, user, simulation)
                else:
                    fn = signal_plugins.get(plugin, self.default_signal)
                    signal = await fn(ctx, user=user, simulation=simulation)

                # Integrate the signal with dynamic TP/SL calculation
                signal_data = await self._process_signal_for_tp_sl(ctx, signal)

                signal["trace_id"] = trace_id
                rationale = signal.get("note", "default rationale")
                audit = {
                    "ctx": ctx,
                    "signal": signal,
                    "user": user,
                    "simulation": simulation,
                    "trace_id": trace_id,
                    "rationale": rationale,
                    "audit_hash": signal_hash(ctx, signal),
                }

                await self._fire_event("signal_generated", audit)
                self.last_signal = audit

                if signal.get("alert"):
                    SIGNAL_ALERTS.inc()
                    self.alerts.append(audit)
                    await self._fire_event("signal_alert", audit)

                return audit

        except Exception as e:
            SIGNAL_ERRORS.inc()
            SIGNAL_DLQ.inc()
            err_data = {"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id}
            FAILED_SIGNAL_DLQ.append(err_data)
            await self._fire_event("signal_error", err_data)
            logger.error(f"[SignalEngineV3] Signal generation error: {e}")
            return {"error": str(e), "user": user, "trace_id": trace_id}

    async def ensemble_signal(self, ctx: Dict[str, Any], user: Optional[str], simulation: bool) -> Dict[str, Any]:
        results = []
        for plugin in ensemble_plugins:
            fn = signal_plugins.get(plugin)
            if fn:
                try:
                    result = await fn(ctx, user=user, simulation=simulation)
                    results.append((plugin, result))
                except Exception as e:
                    logger.warning(f"Plugin {plugin} failed: {e}")
        return combine_ensemble(results)

    async def default_signal(self, ctx: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        trend = ctx.get("trend_score", 0.0)
        sentiment = ctx.get("sentiment_score", 0.0)
        whales = ctx.get("whale_score", 0.0)
        liquidity = ctx.get("liquidity_score", 0.0)
        composite = (0.3 * trend) + (0.3 * sentiment) + (0.2 * whales) + (0.2 * liquidity)
        note = "Strong Buy" if composite > 0.75 else "Strong Sell" if composite < -0.4 else "Hold"
        return {
            "signal_strength": composite,
            "alert": composite > 0.85 or composite < -0.5,
            "note": note,
            "components": {
                "trend": trend,
                "sentiment": sentiment,
                "whales": whales,
                "liquidity": liquidity
            },
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation,
        }

    async def _process_signal_for_tp_sl(self, ctx: Dict[str, Any], signal: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the signal data to integrate dynamic TP/SL calculations.
        Uses AdaptiveTPSL for live adjustments based on the signal context.
        """
        signal_context = {
            "price": signal.get("price"),
            "volatility": signal.get("volatility", 0.02),
            "trend_strength": signal.get("trend", 0.0),
            "whale_pressure": signal.get("whale_pressure", 0.0),
            "spread": signal.get("spread", 0.001),
            "gas_cost": signal.get("gas_cost", 0.0),
            "orderbook_bias": signal.get("orderbook_bias", 0.0),
            "rug_score": signal.get("rug_score", 0.0),
            "cycle_phase": signal.get("cycle_phase", 0.5),
            "user_risk": signal.get("user_risk", 0.0)
        }

        # Use the AdaptiveTPSL class to compute dynamic TP/SL based on the signal
        tp_sl = await self.adaptive_tp_sl.compute(ctx.get("symbol"), signal.get("price"), signal_context)

        # Append the TP/SL data to the result
        signal["tp"] = tp_sl["tp"]
        signal["sl"] = tp_sl["sl"]
        signal["rr_ratio"] = tp_sl["rr_ratio"]
        signal["size"] = tp_sl["size"]

        return signal

    async def replay_dlq(self):
        for failed in FAILED_SIGNAL_DLQ[:]:
            try:
                _ = await self.generate_signal(failed.get("ctx", {}), user=failed.get("user"))
                FAILED_SIGNAL_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_SIGNAL_DLQ.copy()

    def get_alerts(self):
        return self.alerts

    def get_last_signal(self):
        return self.last_signal

    async def _fire_event(self, name: str, data: dict):
        for hook in signal_event_hooks:
            await hook(name, data)
