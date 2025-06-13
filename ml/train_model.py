# Elite Upgrade: ml/train_model.py
# - Real data pipeline
# - Metrics & telemetry
# - Audit hash/provenance
# - Ops/CI safe: never fakes, logs failures

import os
import joblib
import numpy as np
from datetime import datetime
from sklearn.ensemble import RandomForestClassifier
from prometheus_client import Counter, Histogram
import hashlib
import logging

# === Metrics ===
TRAIN_CALLS = Counter("train_model_calls_total", "Total training runs")
TRAIN_ERRORS = Counter("train_model_errors_total", "Training errors")
TRAIN_LATENCY = Histogram("train_model_latency_seconds", "Training time latency")

log = logging.getLogger("train_model")

DATA_PATH = os.getenv("ML_SIGNAL_TRAIN_PATH", "logs/train_signals.csv")
MODEL_PATH = os.getenv("ML_SIGNAL_MODEL_PATH", "ml/model.pkl")

def audit_hash(X, y) -> str:
    """Hash the dataset used for training (for provenance, audit, CI trace)."""
    m = hashlib.sha256()
    m.update(np.array(X).tobytes())
    m.update(np.array(y).tobytes())
    m.update(str(datetime.utcnow()).encode())
    return m.hexdigest()

def load_training_data():
    # User must ensure logs/train_signals.csv or a real path exists (no fake/mocks!)
    if not os.path.exists(DATA_PATH):
        log.error(f"[train_model] Training data file not found at {DATA_PATH}")
        raise FileNotFoundError(f"Training data file missing: {DATA_PATH}")
    import pandas as pd
    try:
        df = pd.read_csv(DATA_PATH)
        # Expect: columns [price, volume, volatility, label]
        if not all(col in df.columns for col in ["price", "volume", "volatility", "label"]):
            raise ValueError("Training data missing required columns.")
        X = df[["price", "volume", "volatility"]].values
        y = df["label"].astype(int).values
        return X, y
    except Exception as e:
        log.error(f"[train_model] Failed to load or parse training data: {e}")
        raise

def train_and_save_model():
    TRAIN_CALLS.inc()
    import time
    start = time.monotonic()
    try:
        with TRAIN_LATENCY.time():
            X, y = load_training_data()
            model = RandomForestClassifier(n_estimators=100, random_state=42)
            model.fit(X, y)
            os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
            joblib.dump(model, MODEL_PATH)
            h = audit_hash(X, y)
            log.info(f"✅ Model trained and saved to {MODEL_PATH} | audit_hash={h}")
            print(f"✅ Model trained and saved to {MODEL_PATH} | audit_hash={h}")
    except Exception as e:
        TRAIN_ERRORS.inc()
        log.error(f"[train_model] Training failed: {e}")
        print(f"❌ Model training failed: {e}")
        raise

if __name__ == "__main__":
    train_and_save_model()
