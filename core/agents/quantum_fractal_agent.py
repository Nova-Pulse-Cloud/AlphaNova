import asyncio
import logging
import hashlib
import json
import numpy as np
from typing import List, Dict, Any, Optional
from datetime import datetime
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field
import aioredis

logger = logging.getLogger("quantum_fractal_agent")

# === Prometheus metrics ===
FRACTAL_DETECT_REQUESTS = Counter("fractal_detection_requests_total", "Total fractal detection requests")
FRACTAL_SIGNAL_EMITTED = Counter("fractal_signal_emitted_total", "Total quantum fractal signals emitted")
FRACTAL_LATENCY = Histogram("fractal_detection_latency_seconds", "Latency of quantum fractal computations")
FRACTAL_DLQ = Counter("fractal_dlq_total", "Quantum fractal DLQ events")
FRACTAL_ANOMALY = Counter("fractal_anomaly_total", "Quantum fractal anomaly events")

# === DLQ, event hooks, plugin registry ===
FAILED_FRACTAL_DLQ = []
fractal_event_hooks = []

def register_fractal_event_hook(hook_fn):
    fractal_event_hooks.append(hook_fn)

async def fire_fractal_event(event_name, data):
    for hook in fractal_event_hooks:
        await hook(event_name, data)

fractal_plugins = {}
def register_fractal_plugin(name: str, fn):
    fractal_plugins[name] = fn

# === Fractal Signal Model ===
class FractalSignal(BaseModel):
    symbol: str
    fractal_type: str
    confidence: float = Field(..., ge=0.0, le=1.0)
    signal_strength: float
    trust_score: float
    meta_caption: str
    points: List[float]
    timestamp: int
    audit_hash: str
    detected_by: str
    anomalous: bool = False
    user: Optional[str] = None

# RL/adaptive feedback (extend for RL mesh)
async def fractal_rl_feedback(features: Dict[str, Any], signal: Dict[str, Any], outcome: Dict[str, Any]):
    await fire_fractal_event("rl_feedback", {"features": features, "signal": signal, "outcome": outcome})

def is_anomalous_fractal(signal: dict) -> bool:
    if signal["confidence"] < 0.3 or signal["signal_strength"] < 1: return True
    if len(signal["points"]) < 6: return True
    return False

def fractal_hash(signal: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(signal, sort_keys=True).encode())
    return m.hexdigest()

# === Elite Quantum Fractal Agent ===
class QuantumFractalAgent:
    def __init__(self, redis: aioredis.Redis, openai_client):
        """Inject aioredis client and openai.Async client for production wiring."""
        self.redis = redis
        self.openai_client = openai_client

    async def analyze(self, symbol: str, price_series: List[float], user: Optional[str] = None, force_refresh: bool = False) -> FractalSignal:
        FRACTAL_DETECT_REQUESTS.inc()
        ts = int(datetime.utcnow().timestamp())
        key_hash = hashlib.sha256(json.dumps(price_series).encode()).hexdigest()
        cache_key = f"fractal:{symbol}:{key_hash}:{user or 'global'}"
        cached = await self.redis.get(cache_key)
        if cached and not force_refresh:
            return FractalSignal.parse_raw(cached)

        try:
            async with FRACTAL_LATENCY.time():
                # Try plugins first
                signal = None
                for name, plugin in fractal_plugins.items():
                    try:
                        signal = await plugin(symbol, price_series)
                        signal["meta_caption"] = signal.get("meta_caption", "")
                        signal["detected_by"] = name
                        break
                    except Exception as e:
                        continue
                if signal is None:
                    signal = await self._default_fractal_detect(symbol, price_series)
                    signal["detected_by"] = "default"

                signal["user"] = user
                signal["anomalous"] = is_anomalous_fractal(signal)
                signal["timestamp"] = ts
                signal["audit_hash"] = fractal_hash(signal)
                await self.redis.set(cache_key, FractalSignal(**signal).json(), ex=600)

                if signal["anomalous"]:
                    FRACTAL_ANOMALY.inc()
                    FRACTAL_DLQ.inc()
                    FAILED_FRACTAL_DLQ.append(signal)
                    await fire_fractal_event("fractal_anomaly", signal)
                else:
                    FRACTAL_SIGNAL_EMITTED.inc()
                    await fire_fractal_event("fractal_signal", signal)
                
                return FractalSignal(**signal)
        except Exception as e:
            FRACTAL_DLQ.inc()
            FAILED_FRACTAL_DLQ.append({"symbol": symbol, "error": str(e), "user": user})
            await fire_fractal_event("fractal_failed", {"symbol": symbol, "error": str(e), "user": user})
            return FractalSignal(
                symbol=symbol, fractal_type="unknown", confidence=0.0,
                signal_strength=0.0, trust_score=0.0, meta_caption="fail",
                points=price_series[:5], timestamp=ts, audit_hash="fail", detected_by="error", anomalous=True, user=user
            )

    async def _default_fractal_detect(self, symbol: str, price_series: List[float]) -> dict:
        points = price_series[-12:]
        delta = (points[-1] - points[0]) if len(points) > 1 else 0.0
        confidence = min(1.0, max(0.0, abs(delta) / max(0.0001, np.std(points) + 1e-4)))
        signal_strength = float(np.std(points))
        trust_score = 0.5 + 0.5 * confidence
        meta_caption = await self._llm_caption(symbol, points, confidence, signal_strength, trust_score)
        return {
            "symbol": symbol,
            "fractal_type": "quantum",
            "confidence": confidence,
            "signal_strength": signal_strength,
            "trust_score": trust_score,
            "meta_caption": meta_caption,
            "points": points
        }

    async def _llm_caption(self, symbol: str, points: List[float], conf: float, strength: float, trust: float) -> str:
        prompt = (
            f"Analyze the following time series fractal pattern for {symbol}: {points}. "
            f"Confidence: {conf:.2f}, Strength: {strength:.2f}, Trust: {trust:.2f}. "
            f"Describe the most likely future direction and any anomaly risk."
        )
        try:
            res = await self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=60
            )
            return res.choices[0].message.content.strip()
        except Exception as e:
            return f"LLM caption error: {e}"

    async def replay_dlq(self):
        for failed in FAILED_FRACTAL_DLQ[:]:
            try:
                _ = await self.analyze(failed["symbol"], failed.get("points", []), failed.get("user"), force_refresh=True)
                FAILED_FRACTAL_DLQ.remove(failed)
            except:
                pass

    def get_dlq(self):
        return FAILED_FRACTAL_DLQ.copy()
