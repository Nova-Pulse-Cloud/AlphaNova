# File: core/signal_generation/sl_manager.py
# 🚀 Nova Pulse ALPHA – SL Manager: Unified TP/SL Routing and Benchmarking

import asyncio
import logging
from typing import Dict, Any, Optional, Tuple, List, Callable, Awaitable
from prometheus_client import Counter, Histogram

from core.signal_generation.ml_tp_sl import extract_features, predict_tp_sl, tpsl_rl_feedback as ml_feedback
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL

logger = logging.getLogger("nova.sl_manager")

# === Prometheus Metrics ===
SLM_CALLS      = Counter("slm_total_calls", "SL Manager TP/SL invocations")
SLM_ERRORS     = Counter("slm_total_errors", "SL Manager errors")
SLM_LATENCY    = Histogram("slm_latency_seconds", "TP/SL route latency")
SLM_CACHE_HITS = Counter("slm_cache_hits_total", "SL Manager Cache Hits")

# === Plugin registry for alternate TP/SL logic ===
slm_plugins: Dict[str, Callable[[Dict[str, Any]], Awaitable[Tuple[float, float, dict]]]] = {}


def register_slm_plugin(name: str, fn: Callable[[Dict[str, Any]], Awaitable[Tuple[float, float, dict]]]):
    slm_plugins[name] = fn

# === Initialize Adaptive TPSL Instance ===
adaptive_tp_sl = AdaptiveTPSL()  # Assumes defaults or injected dependencies

# === Main: Unified TP/SL Routing ===
async def get_best_tp_sl(
    signal_ctx: Dict[str, Any],
    method: str = "auto",
    benchmark: bool = False
) -> Tuple[float, float, dict]:
    SLM_CALLS.inc()
    trace_id = signal_ctx.get("trace_id") or f"slm-{signal_ctx.get('symbol','unk')}-{id(signal_ctx)%10000}"
    cache = signal_ctx.setdefault('cache', {})
    cache_key = f"{trace_id}-{method}"

    # === Check Cache ===
    if cache_key in cache:
        SLM_CACHE_HITS.inc()
        cached = cache[cache_key]
        return cached["tp_price"], cached["sl_price"], cached

    try:
        async with SLM_LATENCY.time():
            results: List[Tuple[float, float, dict, str]] = []

            if method in ("auto", "ml_tp_sl", "adaptive_tp_sl"):
                # ML TP/SL
                if method in ("auto", "ml_tp_sl"):
                    try:
                        features = extract_features(signal_ctx)
                        tp_ml, sl_ml, meta_ml = await predict_tp_sl(features, user=signal_ctx.get("user_id"))
                        results.append((tp_ml, sl_ml, meta_ml, "ml_tp_sl"))
                    except Exception as e:
                        SLM_ERRORS.inc()
                        logger.warning(f"ML TP/SL failed: {e}")

                # Adaptive TP/SL
                if method in ("auto", "adaptive_tp_sl"):
                    try:
                        tp_ad, sl_ad, meta_ad = await adaptive_tp_sl.compute(
                            signal_ctx.get("symbol"),
                            signal_ctx.get("entry_price"),
                            signal_ctx
                        )
                        results.append((tp_ad, sl_ad, meta_ad, "adaptive_tp_sl"))
                    except Exception as e:
                        SLM_ERRORS.inc()
                        logger.warning(f"Adaptive TP/SL failed: {e}")

                # External Plugins
                for name, fn in slm_plugins.items():
                    try:
                        tp_p, sl_p, meta_p = await fn(signal_ctx)
                        results.append((tp_p, sl_p, meta_p, name))
                    except Exception as e:
                        SLM_ERRORS.inc()
                        logger.warning(f"Plugin {name} failed: {e}")

                if not results:
                    raise RuntimeError("No TP/SL method succeeded")

                # Rank: minimize expected_loss, maximize confidence
                ranked = sorted(
                    results,
                    key=lambda x: (x[2].get("expected_loss", 1.0), -x[2].get("confidence", 0))
                )
                tp, sl, meta, plugin = ranked[0]
                output = {
                    "tp_price": tp,
                    "sl_price": sl,
                    "plugin": plugin,
                    "meta": meta,
                    **({"benchmark": [
                        {"plugin": nm, "tp": t, "sl": s, "meta": m}
                        for (t, s, m, nm) in results
                    ]} if benchmark else {}),
                    "trace_id": trace_id
                }

            elif method in slm_plugins:
                tp, sl, meta = await slm_plugins[method](signal_ctx)
                output = {"tp_price": tp, "sl_price": sl, "plugin": method, **meta, "trace_id": trace_id}

            else:
                raise ValueError(f"Unknown method: {method}")

            # Cache & return
            cache[cache_key] = output
            return output["tp_price"], output["sl_price"], output

    except Exception as e:
        SLM_ERRORS.inc()
        logger.error(f"Error in TP/SL calculation: {e}")
        fallback = signal_ctx.get("entry_price", 1.0)
        return fallback * 1.07, fallback * 0.96, {"plugin": "fallback", "error": str(e), "trace_id": trace_id}

# === Feedback Interface ===
async def slm_feedback(signal_ctx: Dict[str, Any], outcome: Dict[str, Any]):
    # ML feedback
    try:
        features = extract_features(signal_ctx)
        await ml_feedback(features, outcome)
    except Exception:
        pass

    # Adaptive feedback
    try:
        await adaptive_tp_sl.tpsl_rl_feedback(signal_ctx, outcome)
    except Exception:
        pass

# === Utility ===
def list_slm_plugins() -> List[str]:
    return ["ml_tp_sl", "adaptive_tp_sl"] + list(slm_plugins.keys())
