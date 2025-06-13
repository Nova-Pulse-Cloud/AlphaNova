import asyncio
import logging
import hashlib
import json
import uuid
from typing import Dict, Any, Optional, Callable
from prometheus_client import Counter, Histogram
import openai
from core.adaptive_tp_sl import AdaptiveTPSL  # Integrating AdaptiveTP/SL for dynamic trade management
from core.orderbook_overlay import OrderBookOverlay  # Integrating OrderBookOverlay for market depth analysis

logger = logging.getLogger("signal_explainer_ai")

# === Prometheus metrics ===
EXPLAIN_CALLS = Counter("explain_calls_total", "Signal explain requests")
EXPLAIN_ERRORS = Counter("explain_errors_total", "Signal explain errors")
EXPLAIN_LATENCY = Histogram("explain_latency_seconds", "Signal explain latency")
EXPLAIN_DLQ = Counter("explain_dlq_total", "Signal explain DLQ events")
EXPLAIN_ALERTS = Counter("explain_alert_total", "Signal explain alert triggers")

# === DLQ, event hooks, plugin registry ===
FAILED_EXPLAIN_DLQ = []
explain_event_hooks = []

def register_explain_event_hook(hook_fn):
    explain_event_hooks.append(hook_fn)

async def fire_explain_event(event_name, data):
    for hook in explain_event_hooks:
        await hook(event_name, data)

explain_plugins = {}

def register_explain_plugin(name: str, fn: Callable):
    explain_plugins[name] = fn

# === Audit hash ===
def explain_hash(signal: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(signal, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class SignalExplainerAI:
    def __init__(self, openai_api_key: str, adaptive_tp_sl: AdaptiveTPSL, orderbook_overlay: OrderBookOverlay, cache_ttl: int = 60):
        self.last_explanation = None
        self.alerts = []
        self.loop = asyncio.get_event_loop()
        self.cache = {}
        self.cache_ttl = cache_ttl
        self.adaptive_tp_sl = adaptive_tp_sl  # Initialize AdaptiveTP/SL for dynamic TP/SL logic
        self.orderbook_overlay = orderbook_overlay  # Initialize OrderBookOverlay for liquidity analysis
        openai.api_key = openai_api_key  # Set OpenAI API key

    async def explain_signal(self, signal: Dict[str, Any], plugin: str = "default", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        EXPLAIN_CALLS.inc()
        trace_id = signal.get("trace_id") or f"expl-{uuid.uuid4().hex[:8]}"
        
        # Cache check for previously explained signals
        cache_key = f"explain_{trace_id}"
        if cache_key in self.cache and time.time() - self.cache[cache_key]["ts"] < self.cache_ttl:
            return self.cache[cache_key]["value"]

        try:
            async with EXPLAIN_LATENCY.time():
                fn = explain_plugins.get(plugin, self.default_explainer)
                explanation = await fn(signal, user=user, simulation=simulation)

                # Integrating Adaptive TP/SL for signal-based trade management
                signal_data = await self._process_for_tp_sl(signal, explanation)

                rationale = signal.get("note", explanation.get("narrative", "No rationale"))
                audit = {
                    "signal": signal,
                    "explanation": explanation,
                    "user": user,
                    "simulation": simulation,
                    "trace_id": trace_id,
                    "rationale": rationale,
                    "audit_hash": explain_hash(signal, explanation)
                }

                # Cache the explanation
                self.cache[cache_key] = {"ts": time.time(), "value": audit}

                await fire_explain_event("explanation_generated", audit)
                self.last_explanation = audit
                if explanation.get("alert"):
                    EXPLAIN_ALERTS.inc()
                    self.alerts.append(audit)
                    await fire_explain_event("explanation_alert", audit)

                return audit
        except Exception as e:
            EXPLAIN_ERRORS.inc()
            EXPLAIN_DLQ.inc()
            err_data = {"signal": signal, "error": str(e), "user": user, "trace_id": trace_id}
            FAILED_EXPLAIN_DLQ.append(err_data)
            await fire_explain_event("explain_error", err_data)
            logger.error(f"[SignalExplainerAI] Explanation error: {e}")
            return {"error": str(e), "user": user, "trace_id": trace_id}

    async def default_explainer(self, signal: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        trend = signal.get("components", {}).get("trend", 0.0)
        sentiment = signal.get("components", {}).get("sentiment", 0.0)
        whales = signal.get("components", {}).get("whales", 0.0)
        liquidity = signal.get("components", {}).get("liquidity", 0.0)
        confidence = abs(signal.get("signal_strength", 0.0))
        note = signal.get("note", "No context")
        narrative = (
            f"Trend:{trend:+.2f}, Sentiment:{sentiment:+.2f}, Whales:{whales:+.2f}, "
            f"Liquidity:{liquidity:+.2f} — "
            f"Signal: {note}, Confidence: {confidence:.2f}."
        )
        alert = confidence > 0.9 or abs(trend) > 0.7
        return {
            "narrative": narrative,
            "confidence": confidence,
            "alert": alert,
            "timestamp": signal.get("timestamp"),
            "simulation": simulation
        }

    async def _process_for_tp_sl(self, signal: Dict[str, Any], explanation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process the fetched signal data for integration into the adaptive TP/SL system.
        Uses the AdaptiveTPSL class to calculate dynamic TP/SL based on the signal.
        """
        signal_context = {
            "price": explanation.get("price"),
            "volatility": explanation.get("volatility", 0.02),
            "trend_strength": explanation.get("trend", 0.0),
            "whale_pressure": explanation.get("whale_pressure", 0.0),
            "spread": explanation.get("spread", 0.001),
            "gas_cost": explanation.get("gas_cost", 0.0),
            "orderbook_bias": explanation.get("orderbook_bias", 0.0),
            "rug_score": explanation.get("rug_score", 0.0),
            "cycle_phase": explanation.get("cycle_phase", 0.5),
            "user_risk": explanation.get("user_risk", 0.0)
        }
        # Use the AdaptiveTPSL class to compute dynamic TP/SL based on the signal
        tp_sl = await self.adaptive_tp_sl.compute(signal.get("symbol"), explanation.get("price"), signal_context)
        
        # Append the TP/SL data to the explanation
        explanation["tp"] = tp_sl["tp"]
        explanation["sl"] = tp_sl["sl"]
        explanation["rr_ratio"] = tp_sl["rr_ratio"]
        explanation["size"] = tp_sl["size"]

        return explanation

    async def replay_dlq(self):
        for failed in FAILED_EXPLAIN_DLQ[:]:
            try:
                _ = await self.explain_signal(failed.get("signal", {}), user=failed.get("user"))
                FAILED_EXPLAIN_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_EXPLAIN_DLQ.copy()

    def get_alerts(self):
        return self.alerts

    def get_last_explanation(self):
        return self.last_explanation
