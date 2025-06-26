"""
Nova Pulse ML TP/SL Predictor:
- Async, plugin-driven model chain
- Local sklearn fallback with joblib
- Prometheus metrics and DLQ
- Audit hash for reproducibility
- RL feedback hooks for continuous improvement
"""
import os
import json
import asyncio
import logging
import hashlib
from typing import Any, Dict, List, Tuple, Callable, Optional

import joblib
import numpy as np
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.ml_tp_sl")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
ML_TPSL_CALLS    = Counter("ml_tpsl_calls_total", "TP/SL prediction calls")
ML_TPSL_ERRORS   = Counter("ml_tpsl_errors_total", "TP/SL errors/fallbacks")
ML_TPSL_LATENCY  = Histogram("ml_tpsl_latency_seconds", "TP/SL prediction latency")
ML_TPSL_DLQ      = Counter("ml_tpsl_dlq_total", "TP/SL DLQ events")
ML_TPSL_FEEDBACK = Counter("ml_tpsl_feedback_total", "TP/SL RL feedback received")

# ────────────────────────────────────────────────────────────────────────────────
# DLQ and Hooks
# ────────────────────────────────────────────────────────────────────────────────
_FAILED_DLQ: List[Dict[str, Any]] = []
_TPSL_HOOKS: List[Callable[[str, Dict[str, Any]], Any]] = []

def register_tpsl_hook(fn: Callable[[str, Dict[str, Any]], Any]) -> None:
    _TPSL_HOOKS.append(fn)

async def _fire_tpsl_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in list(_TPSL_HOOKS):
        try:
            r = hook(event, payload)
            if asyncio.iscoroutine(r):
                await r
        except Exception as exc:
            logger.warning(f"[ML_TPSL] Hook error on {event}: {exc}")

# ────────────────────────────────────────────────────────────────────────────────
# Plugin registry
# ────────────────────────────────────────────────────────────────────────────────
_TPSL_PLUGINS: Dict[str, Callable[[Dict[str, Any], Optional[str]], Any]] = {}

def register_tpsl_plugin(name: str, fn: Callable[[Dict[str, Any], Optional[str]], Any]) -> None:
    _TPSL_PLUGINS[name] = fn

# ────────────────────────────────────────────────────────────────────────────────
# Helpers stubs: extract_features & feedback hook
# ────────────────────────────────────────────────────────────────────────────────
def extract_features(ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "entry_price": ctx.get("entry", 0.0),
        "volatility": ctx.get("volatility", 0.0),
        "trend_strength": ctx.get("trend_strength", 0.0),
        "liquidity": ctx.get("liquidity", 0.0),
        "whale_pressure": ctx.get("whale_pressure", 0.0),
    }

async def tpsl_rl_feedback(features: Dict[str, Any], outcome: Dict[str, Any]) -> None:
    ML_TPSL_FEEDBACK.inc()
    # placeholder for RL feedback logic

# ────────────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────────────
_MODEL_PATH  = config.TPSL_MODEL_PATH
_FALLBACK_TP = config.DEFAULT_TP_PCT
_FALLBACK_SL = config.DEFAULT_SL_PCT
_CACHE: Dict[str, Dict[str, Any]] = {}

# ────────────────────────────────────────────────────────────────────────────────
# Main prediction
# ────────────────────────────────────────────────────────────────────────────────
async def predict_tp_sl(
    features: Dict[str, Any],
    user: Optional[str] = None
) -> Tuple[float, float, Dict[str, Any]]:
    ML_TPSL_CALLS.inc()
    key = json.dumps(features, sort_keys=True)
    if key in _CACHE:
        return (_CACHE[key]["tp_price"], _CACHE[key]["sl_price"], _CACHE[key])

    result: Dict[str, Any] = {}
    try:
        with ML_TPSL_LATENCY.time():
            # plugin chain
            for name, fn in _TPSL_PLUGINS.items():
                try:
                    out = fn(features, user)
                    if asyncio.iscoroutine(out):
                        out = await out
                    tp_pct, sl_pct, meta = out
                    result = {"tp_pct": tp_pct, "sl_pct": sl_pct, "plugin": name, **(meta or {})}
                    break
                except Exception:
                    continue

            if not result:
                # fallback to local model or defaults
                if os.path.exists(_MODEL_PATH):
                    model = joblib.load(_MODEL_PATH)
                    X = np.array([[features.get(k, 0.0) for k in (
                        "entry_price", "volatility", "trend_strength", "liquidity", "whale_pressure"
                    )]])
                    preds = model.predict(X)[0]
                    result = {"tp_pct": float(preds[0]), "sl_pct": float(preds[1]), "plugin": "sklearn"}
                else:
                    result = {"tp_pct": _FALLBACK_TP, "sl_pct": _FALLBACK_SL, "plugin": "fallback"}
    except Exception as exc:
        ML_TPSL_ERRORS.inc()
        ML_TPSL_DLQ.inc()
        entry = {"features": features, "error": str(exc)}
        _FAILED_DLQ.append(entry)
        await _fire_tpsl_event("error", entry)
        result = {"tp_pct": _FALLBACK_TP, "sl_pct": _FALLBACK_SL, "plugin": "fallback"}

    entry_price = float(features.get("entry_price", 0.0))
    tp_price = round(entry_price * (1 + result["tp_pct"]), 8)
    sl_price = round(entry_price * (1 - result["sl_pct"]), 8)
    result.update({"tp_price": tp_price, "sl_price": sl_price})
    result["audit_hash"] = hashlib.sha256(json.dumps(result, sort_keys=True).encode()).hexdigest()
    _CACHE[key] = result

    await _fire_tpsl_event("prediction", {"features": features, **result})
    return tp_price, sl_price, result
