# Elite: ml/model_trainer.py
# Async/telemetry, audit, event mesh, plugin training, production-grade fit/save

import os
import json
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from ml.feature_engineer import extract_features, to_vector
from prometheus_client import Counter, Histogram

import logging
from typing import Callable, Optional, List

logger = logging.getLogger("model_trainer")

DATA_PATH = "logs/training_data.csv"
MODEL_PATH = "models/tp_sl_predictor.pkl"

# === Metrics ===
MODEL_TRAIN_RUNS = Counter("model_train_runs_total", "Total training runs")
MODEL_TRAIN_ERRORS = Counter("model_train_errors_total", "Training errors")
MODEL_TRAIN_LATENCY = Histogram("model_train_latency_seconds", "Training latency")

# === Event/Plugin mesh ===
model_trainer_hooks: List[Callable] = []
def register_model_trainer_hook(fn: Callable):
    model_trainer_hooks.append(fn)

async def fire_model_trainer_event(event_name, data):
    for hook in model_trainer_hooks:
        await hook(event_name, data)

# === Audit hash ===
def trainer_audit_hash(meta: dict) -> str:
    import hashlib
    return hashlib.sha256(json.dumps(meta, sort_keys=True).encode()).hexdigest()

def load_training_data() -> pd.DataFrame:
    if not os.path.exists(DATA_PATH):
        logger.error(f"[Data] Training data file not found: {DATA_PATH}")
        return pd.DataFrame()
    rows = []
    with open(DATA_PATH, "r") as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return pd.DataFrame(rows)

def train_model(
    model_path: Optional[str] = None,
    data_path: Optional[str] = None,
    plugin_train: Optional[Callable] = None
):
    MODEL_TRAIN_RUNS.inc()
    model_path = model_path or MODEL_PATH
    data_path = data_path or DATA_PATH

    import time
    start = time.monotonic()
    try:
        with MODEL_TRAIN_LATENCY.time():
            df = load_training_data()
            if df.empty:
                logger.error("[Data] No valid data found for training.")
                return

            df = df[df["result"].isin(["win", "loss"])]
            if df.empty:
                logger.error("[Data] No qualifying win/loss data.")
                return

            # Label engineering (expand for RL/reward if needed)
            df["label_tp"] = df["result"].apply(lambda r: 0.07 if r == "win" else 0.02)
            df["label_sl"] = df["result"].apply(lambda r: 0.03 if r == "win" else 0.06)

            # Feature vectors
            try:
                X = df.apply(lambda row: to_vector(extract_features(row))[0], axis=1).tolist()
                y = df[["label_tp", "label_sl"]].values
            except Exception as e:
                logger.error(f"[Features] Failed to create vectors: {e}")
                MODEL_TRAIN_ERRORS.inc()
                return

            # Plugin training
            if plugin_train:
                model = plugin_train(X, y)
            else:
                model = RandomForestRegressor(n_estimators=100, random_state=42)
                model.fit(X, y)

            os.makedirs(os.path.dirname(model_path), exist_ok=True)
            joblib.dump(model, model_path)
            meta = {
                "count": len(df),
                "model_path": model_path,
                "features": list(df.columns),
                "audit_hash": trainer_audit_hash({"count": len(df), "model_path": model_path}),
                "duration": round(time.monotonic() - start, 3)
            }
            logger.info(f"[Trainer] Model trained and saved: {model_path} (samples={len(df)})")
            # Fire mesh event (async-safe)
            try:
                import asyncio
                asyncio.run(fire_model_trainer_event("model_trained", meta))
            except Exception:
                pass
            print(f"[✓] Model trained and saved to: {model_path}")

    except Exception as e:
        logger.error(f"[Trainer] Training failed: {e}")
        MODEL_TRAIN_ERRORS.inc()
        try:
            import asyncio
            asyncio.run(fire_model_trainer_event("train_error", {"error": str(e)}))
        except Exception:
            pass

if __name__ == "__main__":
    train_model()
