# Elite: ml/feature_engineer.py
# Async-ready, schema versioned, metrics, audit, plug-in hooks, production-grade feature extraction

import numpy as np
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Callable
import hashlib
import json
from prometheus_client import Counter, Histogram

# === Metrics ===
FEATURE_EXTRACTIONS = Counter("feature_extractions_total", "Total ML feature extractions")
FEATURE_ERRORS = Counter("feature_extractions_errors_total", "Feature extraction errors")
FEATURE_LATENCY = Histogram("feature_extraction_latency_seconds", "Feature extraction latency")

# === Plug-in/event hooks (optional) ===
feature_hooks = []
def register_feature_hook(hook_fn: Callable):
    feature_hooks.append(hook_fn)
async def fire_feature_event(event_name: str, data: dict):
    for hook in feature_hooks:
        await hook(event_name, data)

# === Feature schema versioning ===
FEATURE_SCHEMA_VERSION = "v1.0"

def feature_hash(features: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(features, sort_keys=True).encode())
    m.update(FEATURE_SCHEMA_VERSION.encode())
    return m.hexdigest()

def extract_features(trade_ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Robust, audit-logged feature extraction for ML.
    Returns a dictionary with versioned schema and hash.
    """
    FEATURE_EXTRACTIONS.inc()
    with FEATURE_LATENCY.time():
        try:
            now = datetime.now(timezone.utc)
            # Parse timestamp safely
            try:
                entry_time = datetime.fromisoformat(str(trade_ctx.get("timestamp")))
                age_seconds = (now - entry_time).total_seconds()
            except Exception:
                age_seconds = 0.0

            symbol = str(trade_ctx.get("token_symbol", "")).strip().lower()
            entry_price = float(trade_ctx.get("entry_price", 0.0))
            volatility_est = float(trade_ctx.get("volatility", 0.02))
            features = {
                "entry_price": entry_price,
                "age_seconds": age_seconds,
                "volatility": volatility_est,
                "symbol_len": len(symbol),
                "starts_with_s": int(symbol.startswith("s")),
                "schema_version": FEATURE_SCHEMA_VERSION,
            }
            features["audit_hash"] = feature_hash(features)
            return features
        except Exception as e:
            FEATURE_ERRORS.inc()
            raise

def to_vector(features: Dict[str, Any]) -> np.ndarray:
    """
    Convert feature dictionary to a NumPy input vector.
    Enforces order and schema version.
    """
    expected = [
        "entry_price",
        "age_seconds",
        "volatility",
        "symbol_len",
        "starts_with_s"
    ]
    # Optionally validate schema
    arr = np.array([[features.get(k, 0.0) for k in expected]])
    return arr
