import asyncio
from typing import Dict, Any, Optional, Tuple, List, Callable, Awaitable
from prometheus_client import Counter, Histogram
from ml import ml_tp_sl, adaptive_tp_sl

# === Prometheus Metrics ===
SLM_CALLS = Counter("slm_total_calls", "SL Manager TP/SL invocations")
SLM_ERRORS = Counter("slm_total_errors", "SL Manager errors")
SLM_LATENCY = Histogram("slm_latency_seconds", "TP/SL route latency")
SLM_CACHE_HITS = Counter("slm_cache_hits_total", "SL Manager Cache Hits")

# === Plugin registry for alternate TP/SL logic ===
slm_plugins: Dict[str, Callable[[Dict[str, Any]], Awaitable[Tuple[float, float, dict]]]] = {}

def register_slm_plugin(name: str, fn: Callable):
    slm_plugins[name] = fn

# === Main: Unified TP/SL Routing ===
async def get_best_tp_sl(signal_ctx: Dict[str, Any], method: str = "auto", benchmark: bool = False) -> Tuple[float, float, dict]:
    SLM_CALLS.inc()
    trace_id = signal_ctx.get("trace_id") or f"slm-{signal_ctx.get('symbol', 'unknown')}-{id(signal_ctx) % 10000}"
    cache_key = f"{trace_id}-{method}"
    
    # === Caching: Check for existing results ===
    if cache_key in signal_ctx.get('cache', {}):
        cached_result = signal_ctx['cache'][cache_key]
        SLM_CACHE_HITS.inc()
        return cached_result["tp_price"], cached_result["sl_price"], cached_result

    try:
        async with SLM_LATENCY.time():
            results = []

            if method == "auto":
                # ML TP/SL
                try:
                    features = ml_tp_sl.extract_features(signal_ctx)
                    tp_ml, sl_ml, meta_ml = await ml_tp_sl.predict_tp_sl(features, user=signal_ctx.get("user_id"))
                    results.append((tp_ml, sl_ml, meta_ml, "ml_tp_sl"))
                except Exception as e:
                    SLM_ERRORS.inc()
                    logger.warning(f"ML TP/SL failed: {e}")

                # Adaptive TP/SL
                try:
                    tp_ad, sl_ad, meta_ad = await adaptive_tp_sl.adaptive_tp_sl(signal_ctx)
                    results.append((tp_ad, sl_ad, meta_ad, "adaptive_tp_sl"))
                except Exception as e:
                    SLM_ERRORS.inc()
                    logger.warning(f"Adaptive TP/SL failed: {e}")

                # External Plugins
                for name, fn in slm_plugins.items():
                    try:
                        tp, sl, meta = await fn(signal_ctx)
                        results.append((tp, sl, meta, name))
                    except Exception as e:
                        SLM_ERRORS.inc()
                        logger.warning(f"Plugin {name} failed: {e}")
                        continue

                if not results:
                    raise RuntimeError("No TP/SL method succeeded")

                # Rank by lowest expected loss and highest confidence
                ranked = sorted(results, key=lambda x: (x[2].get("expected_loss", 1.0), -x[2].get("confidence", 0)))
                best = ranked[0]
                output = {
                    "tp_price": best[0],
                    "sl_price": best[1],
                    "plugin": best[3],
                    "meta": best[2],
                    "benchmark": [
                        {"plugin": name, "tp": tp, "sl": sl, "meta": meta}
                        for tp, sl, meta, name in results
                    ] if benchmark else None
                }
                
                # === Cache the result ===
                signal_ctx['cache'][cache_key] = output
                return output["tp_price"], output["sl_price"], output

            elif method == "ml_tp_sl":
                features = ml_tp_sl.extract_features(signal_ctx)
                tp, sl, meta = await ml_tp_sl.predict_tp_sl(features, user=signal_ctx.get("user_id"))
                return tp, sl, {"plugin": "ml_tp_sl", **meta}

            elif method == "adaptive_tp_sl":
                tp, sl, meta = await adaptive_tp_sl.adaptive_tp_sl(signal_ctx)
                return tp, sl, {"plugin": "adaptive_tp_sl", **meta}

            elif method in slm_plugins:
                tp, sl, meta = await slm_plugins[method](signal_ctx)
                return tp, sl, {"plugin": method, **meta}

            else:
                raise ValueError(f"Unknown method: {method}")

    except Exception as e:
        SLM_ERRORS.inc()
        fallback_tp = signal_ctx.get("entry_price", 1.0) * 1.07
        fallback_sl = signal_ctx.get("entry_price", 1.0) * 0.96
        logger.error(f"Error in TP/SL calculation: {e}")
        return fallback_tp, fallback_sl, {"plugin": "fallback", "error": str(e), "trace_id": trace_id}

# === Interface ===
async def slm_feedback(signal_ctx: Dict[str, Any], outcome: Dict[str, Any]):
    try:
        features = ml_tp_sl.extract_features(signal_ctx)
        _, _, ml_result = await ml_tp_sl.predict_tp_sl(features)
        await ml_tp_sl.tpsl_rl_feedback(features, ml_result, outcome)
    except Exception:
        pass

    try:
        await adaptive_tp_sl.tpsl_rl_feedback(signal_ctx, outcome)
    except Exception:
        pass

# === Debug Util ===
def list_slm_plugins() -> List[str]:
    return ["ml_tp_sl", "adaptive_tp_sl"] + list(slm_plugins.keys())
