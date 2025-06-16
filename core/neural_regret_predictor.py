# core/neural_regret_predictor.py

import asyncio
import logging
import hashlib
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime
from prometheus_client import Counter, Histogram
from pydantic import BaseModel, Field

logger = logging.getLogger("nova.neural_regret_predictor")
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ─────────────────────────────────────────────────────────────────────────────
NRP_UPDATES    = Counter("nrp_updates_total", "Regret predictor updates")
NRP_ERRORS     = Counter("nrp_errors_total", "Regret predictor errors")
NRP_DLQ        = Counter("nrp_dlq_total", "Regret predictor DLQ")
NRP_LATENCY    = Histogram("nrp_latency_seconds", "Latency of RL regret updates")
NRP_HITS       = Counter("nrp_hits_total", "Local regret hits")
NRP_MISSES     = Counter("nrp_misses_total", "Local regret misses")

# ─────────────────────────────────────────────────────────────────────────────
# Event hooks (e.g., for mesh/federated signals in the future)
# ─────────────────────────────────────────────────────────────────────────────
_nrp_event_hooks: List[Callable[[str, Dict[str, Any]], Any]] = []
FAILED_NRP_DLQ: List[Dict[str, Any]] = []

def register_nrp_event_hook(fn: Callable[[str, Dict[str, Any]], Any]) -> None:
    _nrp_event_hooks.append(fn)

async def _fire_nrp_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in _nrp_event_hooks:
        try:
            result = hook(event, payload)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"[NRP] Event hook {hook.__name__} failed: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────
class RegretRecord(BaseModel):
    symbol: str
    regret: float
    context: Dict[str, Any] = Field(default_factory=dict)
    agent: Optional[str] = None
    ts: float = Field(default_factory=lambda: datetime.utcnow().timestamp())

class NeuralRegretPredictor:
    """
    Async regret predictor with local memory, event bus, metrics, DLQ.
    Ready for federated/mesh RL – upgrade with a few lines.
    """
    def __init__(self):
        self._regret_memory: Dict[str, RegretRecord] = {}  # symbol → RegretRecord
        self._lock = asyncio.Lock()
        self._dlq = FAILED_NRP_DLQ

    async def update(
        self,
        symbol: str,
        agent: str,
        reward: float,
        confidence: float = 1.0,
        context: Optional[Dict[str, Any]] = None,
    ) -> None:
        NRP_UPDATES.inc()
        record = RegretRecord(
            symbol=symbol,
            regret=self._compute_regret(symbol, reward, agent, context or {}),
            context=context or {},
            agent=agent,
        )
        async with self._lock, NRP_LATENCY.time():
            self._regret_memory[symbol] = record
        await _fire_nrp_event("regret_update", record.dict())

    async def predict(
        self,
        symbol: str,
        agent: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
    ) -> float:
        async with self._lock:
            rec = self._regret_memory.get(symbol)
            if rec:
                NRP_HITS.inc()
                return rec.regret
            else:
                NRP_MISSES.inc()
                # Return zero or default regret if no data
                return 0.0

    def get_regret_record(self, symbol: str) -> Optional[RegretRecord]:
        return self._regret_memory.get(symbol)

    def get_memory(self) -> List[Dict[str, Any]]:
        return [r.dict() for r in self._regret_memory.values()]

    def get_dlq(self) -> List[Dict[str, Any]]:
        return list(self._dlq)

    async def replay_dlq(self):
        for entry in list(self._dlq):
            try:
                await self.update(
                    entry.get("symbol", ""),
                    entry.get("agent", ""),
                    entry.get("reward", 0.0),
                    context=entry.get("context", {}),
                )
                self._dlq.remove(entry)
            except Exception as e:
                continue

    def _compute_regret(
        self,
        symbol: str,
        reward: float,
        agent: Optional[str],
        context: Dict[str, Any]
    ) -> float:
        # This is where you can plug in a *real* regret calculation (Bayesian, RL, LSTM, etc.)
        # For now: just use -reward for standard regret (you want to minimize regret!)
        # Advanced logic could use context["market"], or rolling window, etc.
        try:
            return -float(reward)
        except Exception as e:
            NRP_ERRORS.inc()
            self._dlq.append({
                "symbol": symbol, "agent": agent, "reward": reward,
                "context": context, "error": str(e)
            })
            return 0.0
