# 🚀 Nova Pulse ALPHA – core/ml/ml_tp_sl.py
# Pro async, event/plugin mesh, metrics, audit hash, RL feedback, hot-reload, full DLQ, safe feature/IO

import os
import json
import math
import joblib
import numpy as np
import asyncio
from datetime import datetime, timezone
from typing import Dict, Tuple, Any, Optional, Callable
from prometheus_client import Counter, Histogram

# === Metrics ===
ML_TPSL_CALLS = Counter("ml_tpsl_calls_total", "TP/SL prediction calls")
ML_TPSL_ERRORS = Counter("ml_tpsl_errors_total", "TP/SL model errors/fallbacks")
ML_TPSL_LATENCY = Histogram("ml_tpsl_latency_seconds", "TP/SL prediction latency")
ML_TPSL_DLQ = Counter("ml_tpsl_dlq_total", "TP/SL DLQ events")
ML_TPSL_FEEDBACK = Counter("ml_tpsl_feedback_total", "TP/SL RL feedback received")

# === DLQ and event hooks ===
FAILED_TPSL_DLQ = []
tpsl_event_hooks = []
def register_tpsl_hook(hook_fn: Callable):
    tpsl_event_hooks.append(hook_fn)

async def fire_tpsl_event(event_name, data):
    for hook in tpsl_event_hooks:
        await hook(event_name, data)

# === Plugin registry ===
tpsl_plugins: Dict[str, Callable] = {}
def register_tpsl_plugin(name: str, fn: Callable):
    tpsl_plugins[name] = fn

# === Feature cache for performance ===
TPSL_FEATURE_CACHE = {}

MODEL_PATH = "models/tp_sl_predictor.pkl"
DEFAULT_TP_PCT = 0.07
DEFAULT_SL_PCT = 0.04
FALLBACK_VOL = 0.02

# === Audit hash ===
def tpsl_hash(features: dict, result: dict) -> str:
    from hashlib import sha256
    m = sha256()
    m.update(json.dumps(features, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

# === Feature extraction (expandable) ===
def extract_features(trade_ctx: Dict[str, Any], orderbook_data: Dict[str, Any]) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    timestamp = trade_ctx.get("timestamp")
    try:
        entry_time = datetime.fromisoformat(timestamp)
    except Exception:
        entry_time = now
    age_seconds = (now - entry_time).total_seconds()
    
    # Market depth integration
    liquidity = orderbook_data.get("liquidity", 0.0)  # Market liquidity from the OrderBookOverlay
    whale_activity = orderbook_data.get("whale_pressure", 0.0)  # Whale pressure from the OrderBookOverlay

    return {
        "symbol": trade_ctx.get("token_symbol", "UNKNOWN"),
        "entry_price": float(trade_ctx.get("entry_price", 0)),
        "side": trade_ctx.get("side", "buy"),
        "volatility": float(trade_ctx.get("volatility", FALLBACK_VOL)),
        "trend_strength": float(trade_ctx.get("trend_strength", 0.5)),
        "whale_pressure": whale_activity,
        "spread": float(trade_ctx.get("spread", 0.001)),
        "gas_cost": float(trade_ctx.get("gas_cost", 0.0)),
        "age_seconds": age_seconds,
        "user": trade_ctx.get("user_id", "global"),
        "liquidity": liquidity  # Added liquidity as a feature for the model
    }

# === ML-based TP/SL prediction entrypoint ===
async def predict_tp_sl(
    features: Dict[str, Any], use_cloud: bool = False, user: Optional[str] = None, orderbook_data: Optional[Dict[str, Any]] = None
) -> Tuple[float, float, dict]:
    ML_TPSL_CALLS.inc()
    cache_key = json.dumps(features, sort_keys=True)
    if cache_key in TPSL_FEATURE_CACHE:
        cached = TPSL_FEATURE_CACHE[cache_key]
        return cached["tp_price"], cached["sl_price"], cached
    result = {}
    try:
        async with ML_TPSL_LATENCY.time():
            # Try plugin chain first
            for name, plugin in tpsl_plugins.items():
                try:
                    tp_pct, sl_pct, meta = await plugin(features, user)
                    result = {
                        "tp_pct": tp_pct,
                        "sl_pct": sl_pct,
                        "meta": meta or {},
                        "plugin": name
                    }
                    break
                except Exception as e:
                    continue

            if not result:
                if use_cloud:
                    raise NotImplementedError("Cloud ML endpoint not implemented yet.")
                elif os.path.exists(MODEL_PATH):
                    # Run sklearn model locally
                    loop = asyncio.get_event_loop()
                    model = await loop.run_in_executor(None, joblib.load, MODEL_PATH)
                    X = np.array([[features["entry_price"], features["volatility"], features["trend_strength"], features["liquidity"], features["whale_pressure"]]])
                    y_pred = model.predict(X)[0]
                    result = {
                        "tp_pct": float(y_pred[0]),
                        "sl_pct": float(y_pred[1]),
                        "meta": {},
                        "plugin": "sklearn_local"
                    }
                else:
                    # Last resort: static fallback
                    result = {
                        "tp_pct": DEFAULT_TP_PCT,
                        "sl_pct": DEFAULT_SL_PCT,
                        "meta": {},
                        "plugin": "fallback"
                    }
    except Exception as e:
        ML_TPSL_ERRORS.inc()
        ML_TPSL_DLQ.inc()
        FAILED_TPSL_DLQ.append({"features": features, "error": str(e)})
        await fire_tpsl_event("prediction_error", {"features": features, "error": str(e)})
        result = {
            "tp_pct": DEFAULT_TP_PCT,
            "sl_pct": DEFAULT_SL_PCT,
            "meta": {},
            "plugin": "fallback"
        }

    entry = features.get("entry_price", 0.0)
    result["tp_price"] = round(entry * (1 + result["tp_pct"]), 6)
    result["sl_price"] = round(entry * (1 - result["sl_pct"]), 6)
    result["audit_hash"] = tpsl_hash(features, result)
    TPSL_FEATURE_CACHE[cache_key] = result
    await fire_tpsl_event("prediction", {"features": features, **result})
    return result["tp_price"], result["sl_price"], result

# === RL Feedback (applies learning from trade outcome) ===
async def tpsl_rl_feedback(features: Dict[str, Any], result: Dict[str, Any], outcome: Dict[str, Any]):
    ML_TPSL_FEEDBACK.inc()
    await fire_tpsl_event("rl_feedback", {"features": features, "result": result, "outcome": outcome})
    try:
        reward = outcome.get("pnl", 0.0)
        feedback_meta = {
            "symbol": features.get("symbol"),
            "plugin": result.get("plugin"),
            "reward": reward,
            "tp": result.get("tp_price"),
            "sl": result.get("sl_price"),
            "user": features.get("user")
        }
        await fire_tpsl_event("rl_signal", feedback_meta)
    except Exception:
        pass

# === DLQ replay ===
async def replay_dlq():
    for failed in FAILED_TPSL_DLQ[:]:
        try:
            tp, sl, _ = await predict_tp_sl(failed["features"])
            if tp and sl:
                FAILED_TPSL_DLQ.remove(failed)
        except:
            pass

def get_dlq():
    return FAILED_TPSL_DLQ.copy()

# === Jupiter plugin registration (if available) ===
try:
    from utils.tpsl_jupiter_plugin import jupiter_tp_sl_plugin
    register_tpsl_plugin("jupiter", jupiter_tp_sl_plugin)
except Exception as e:
    import logging
    logging.warning(f"[TP/SL] Jupiter plugin not loaded: {e}")
