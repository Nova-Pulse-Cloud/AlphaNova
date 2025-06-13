# Elite: ml/predictor.py
# Async-ready, metrics, model cache, audit, event hooks, no mock/fallback, production ops

import os
import logging
import joblib
import numpy as np
from ml.feature_engineer import extract_features, to_vector
from prometheus_client import Counter, Histogram

log = logging.getLogger("predictor")

MODEL_PATH = "models/tp_sl_predictor.pkl"
_model = None

# === Metrics ===
PREDICT_CALLS = Counter("predictor_calls_total", "Total TP/SL predictions")
PREDICT_ERRORS = Counter("predictor_errors_total", "Prediction errors")
PREDICT_LATENCY = Histogram("predictor_latency_seconds", "Prediction latency")

# === Event hooks ===
predictor_hooks = []
def register_predictor_hook(fn):
    predictor_hooks.append(fn)
async def fire_predictor_event(event_name, data):
    for hook in predictor_hooks:
        await hook(event_name, data)

# === Model loading ===
def load_model():
    global _model
    if _model is None:
        if not os.path.exists(MODEL_PATH):
            log.error(f"[Predictor] Model file missing: {MODEL_PATH}")
            raise FileNotFoundError(f"Model not found at {MODEL_PATH}")
        _model = joblib.load(MODEL_PATH)
        log.info(f"[Predictor] Model loaded from {MODEL_PATH}")
    return _model

# === Predict TP/SL for a trade dict ===
def predict_tp_sl(token_data: dict) -> tuple[float, float]:
    PREDICT_CALLS.inc()
    import time
    start = time.monotonic()
    try:
        with PREDICT_LATENCY.time():
            model = load_model()
            features = to_vector(extract_features(token_data))
            prediction = model.predict(features)
            take_profit, stop_loss = prediction[0]
            meta = {
                "tp_pred": take_profit,
                "sl_pred": stop_loss,
                "token": token_data.get("token_symbol"),
                "audit_hash": _predictor_audit_hash(token_data, prediction[0])
            }
            # Optionally fire hooks (if any async)
            try:
                import asyncio
                asyncio.run(fire_predictor_event("tp_sl_predicted", meta))
            except Exception:
                pass
            return float(take_profit), float(stop_loss)
    except Exception as e:
        PREDICT_ERRORS.inc()
        log.error(f"[Predictor Error] Failed to predict TP/SL: {e}")
        raise

# === Score token for TP only ===
def score_token(token_data: dict) -> float:
    PREDICT_CALLS.inc()
    try:
        model = load_model()
        features = to_vector(extract_features(token_data))
        prediction = model.predict(features)
        meta = {
            "tp_pred": prediction[0][0],
            "token": token_data.get("token_symbol"),
            "audit_hash": _predictor_audit_hash(token_data, prediction[0])
        }
        try:
            import asyncio
            asyncio.run(fire_predictor_event("tp_score", meta))
        except Exception:
            pass
        return float(prediction[0][0])
    except Exception as e:
        PREDICT_ERRORS.inc()
        log.error(f"[Predictor Error] Failed to score token: {e}")
        raise

# === Audit hash for provenance ===
def _predictor_audit_hash(token_data, prediction) -> str:
    import hashlib, json
    return hashlib.sha256(
        (json.dumps(token_data, sort_keys=True) + str(prediction)).encode()
    ).hexdigest()
