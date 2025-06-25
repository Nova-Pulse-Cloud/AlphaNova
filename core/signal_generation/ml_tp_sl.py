# File: ml_tp_sl.py
"""
Nova Pulse ML TP/SL Predictor:
- Async, plugin‑driven model chain
- Local sklearn fallback with joblib
- Prometheus metrics and DLQ
- Audit hash for reproducibility
- RL feedback hooks for continuous improvement
"""
import os
import json
import asyncio
import logging
from datetime import datetime, timezone
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
    """Register a hook for TP/SL events."""
    _TPSL_HOOKS.append(fn)

async def _fire_tpsl_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in list(_TPSL_HOOKS):
        try:
            result = hook(event, payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception as exc:
            logger.warning(f"[ML_TPSL] Hook error on {event}: {exc}")

# ────────────────────────────────────────────────────────────────────────────────
# Plugin registry
# ────────────────────────────────────────────────────────────────────────────────
_TPSL_PLUGINS: Dict[str, Callable[[Dict[str, Any], Optional[str]], Any]] = {}

def register_tpsl_plugin(name: str, fn: Callable[[Dict[str, Any], Optional[str]], Any]) -> None:
    """Register a TP/SL plugin."""
    _TPSL_PLUGINS[name] = fn

# ────────────────────────────────────────────────────────────────────────────────
# Constants
# ────────────────────────────────────────────────────────────────────────────────
_MODEL_PATH      = config.TPSL_MODEL_PATH or "models/tp_sl_predictor.pkl"
_FALLBACK_TP     = config.DEFAULT_TP_PCT or 0.07
_FALLBACK_SL     = config.DEFAULT_SL_PCT or 0.04
_CACHE: Dict[str, Dict[str, Any]] = {}

# ────────────────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────────────────

def _audit_hash(features: Dict[str, Any], result: Dict[str, Any]) -> str:
    import hashlib
    h = hashlib.sha256()
    h.update(json.dumps(features, sort_keys=True).encode())
    h.update(json.dumps(result, sort_keys=True).encode())
    return h.hexdigest()

async def predict_tp_sl(
    features: Dict[str, Any],
    use_cloud: bool = False,
    user: Optional[str] = None
) -> Tuple[float, float, Dict[str, Any]]:
    """
    Predict TP/SL given feature dict.
    Returns (tp_price, sl_price, metadata).
    """
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
                    out = fn(features, user)  # may return coroutine
                    if asyncio.iscoroutine(out):
                        out = await out
                    tp_pct, sl_pct, meta = out
                    result = {"tp_pct": tp_pct, "sl_pct": sl_pct, "plugin": name, **(meta or {})}
                    break
                except Exception:
                    continue

            if not result:
                # local model fallback
                if os.path.exists(_MODEL_PATH):
                    model = joblib.load(_MODEL_PATH)
                    X = np.array([[
                        features.get("entry_price", 0.0),
                        features.get("volatility", 0.0),
                        features.get("trend_strength", 0.0),
                        features.get("liquidity", 0.0),
                        features.get("whale_pressure", 0.0)
                    ]])
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

    # compute prices
    entry_price = float(features.get("entry_price", 0.0))
    tp_price = round(entry_price * (1 + result["tp_pct"]), 8)
    sl_price = round(entry_price * (1 - result["sl_pct"]), 8)
    result.update({"tp_price": tp_price, "sl_price": sl_price})
    result["audit_hash"] = _audit_hash(features, result)
    _CACHE[key] = result

    await _fire_tpsl_event("prediction", {"features": features, **result})
    return tp_price, sl_price, result

async def replay_dlq() -> None:
    """Retry any failed predictions."""
    for entry in list(_FAILED_DLQ):
        try:
            await predict_tp_sl(entry["features"])
            _FAILED_DLQ.remove(entry)
        except Exception:
            pass

def get_dlq() -> List[Dict[str, Any]]:
    return list(_FAILED_DLQ)

# ────────────────────────────────────────────────────────────────────────────────
# Optional: Register Jupiter plugin if available
# ────────────────────────────────────────────────────────────────────────────────
try:
    from core.utils.tpsl_jupiter_plugin import jupiter_tp_sl_plugin
    register_tpsl_plugin("jupiter", jupiter_tp_sl_plugin)
    logger.info("[ML_TPSL] Jupiter plugin registered.")
except ImportError:
    logger.info("[ML_TPSL] Jupiter plugin not found, skipping.")
