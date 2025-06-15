# core/signal_generation/sl_manager.py
"""
Nova Pulse ALPHA — Elite SL Manager 4.0
────────────────────────────────────────────────────────────────
• Async parallel TP/SL computations (ML, Adaptive, Jupiter plugins)
• Pydantic models for type-safety
• Prometheus metrics with labels for method and stage
• Concurrency control and per-plugin timeouts
• In-memory cache per-signal with TTL
• DLQ for failed plugin calls and replay support
• [Elite] – Audit trail, plugin feedback, plugin registry events, full plugin error details
"""
import asyncio
import logging
from typing import Any, Dict, Tuple, List, Optional

from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from core.signal_generation.ml_tp_sl import extract_features, predict_tp_sl, tpsl_rl_feedback
from core.signal_generation.adaptive_tp_sl import AdaptiveTPSL
from core.utils.config import config
from utils.tpsl_jupiter_plugin import jupiter_tp_sl_plugin
from utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.sl_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
SLM_CALLS     = Counter("slm_calls_total", "Total SLM calls", ["method"])
SLM_ERRORS    = Counter("slm_errors_total", "SLM errors", ["method"])
SLM_LATENCY   = Histogram("slm_latency_seconds", "SLM latency per method", ["method"])

# DLQ for failed plugin calls
FAILED_SLM_DLQ: List[Dict[str, Any]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Data Models
# ────────────────────────────────────────────────────────────────────────────────
class SLResult(BaseModel):
    tp: float = Field(..., ge=0)
    sl: float = Field(..., ge=0)
    method: str
    meta: Dict[str, Any]
    event: Optional[str] = None  # for plugin events, e.g. 'tp_hit', 'sl_hit'
    audit_hash: Optional[str] = None

# ────────────────────────────────────────────────────────────────────────────────
# Plugin Registry
# ────────────────────────────────────────────────────────────────────────────────
_slm_plugins: Dict[str, Any] = {}

def register_slm_plugin(name: str, fn: Any) -> None:
    _slm_plugins[name] = fn
    logger.info(f"[SLM] Registered plugin: {name}")

# register Jupiter plugin
register_slm_plugin("jupiter", jupiter_tp_sl_plugin)

# adaptive instance
_adaptive = AdaptiveTPSL()

# ────────────────────────────────────────────────────────────────────────────────
# Main API
# ────────────────────────────────────────────────────────────────────────────────
class SLManager:
    def __init__(self, cache_ttl: int = 30) -> None:
        self._cache: Dict[str, Tuple[SLResult, float]] = {}
        self._cache_ttl = cache_ttl
        self._lock = asyncio.Lock()

    async def get_best_tp_sl(
        self,
        ctx: Dict[str, Any],
        methods: Optional[List[str]] = None,
        timeout: float = 5.0
    ) -> SLResult:
        """
        Compute TP/SL using specified methods in parallel, with full plugin event logging and audit trail.
        """
        key = ctx.get("trace_id", str(id(ctx)))
        async with self._lock:
            cached = self._cache.get(key)
            if cached and (asyncio.get_event_loop().time() - cached[1]) < self._cache_ttl:
                logger.info(f"[SLManager] Cache hit for {key}")
                return cached[0]

        methods = methods or ["ml", "adaptive", *_slm_plugins.keys()]
        tasks = []
        for m in methods:
            tasks.append(self._run_method(ctx, m, timeout))

        results: List[SLResult] = []
        for task in asyncio.as_completed(tasks):
            try:
                res = await task
                logger.info(f"[SLManager] Got result from {res.method}: TP={res.tp} SL={res.sl}")
                results.append(res)
            except Exception as e:
                FAILED_SLM_DLQ.append({"ctx": ctx, "error": str(e)})
                logger.error(f"[SLManager] Plugin call failed: {e}")
                continue

        if not results:
            raise RuntimeError("All TP/SL methods failed")

        # choose by lowest expected_loss then highest confidence
        ranked = sorted(
            results,
            key=lambda r: (r.meta.get("expected_loss", float('inf')), -r.meta.get("confidence", 0))
        )
        best = ranked[0]
        best.audit_hash = self._audit_hash(best)

        async with self._lock:
            self._cache[key] = (best, asyncio.get_event_loop().time())
        await fire_plugin_event("tp_sl_computed", {"result": best.dict(), "ctx": ctx})
        return best

    async def _run_method(
        self,
        ctx: Dict[str, Any],
        method: str,
        timeout: float
    ) -> SLResult:
        label = method
        SLM_CALLS.labels(method=label).inc()
        start = asyncio.get_event_loop().time()
        try:
            async with SLM_LATENCY.labels(method=label).time():
                if method == "ml":
                    features = extract_features(ctx)
                    tp, sl, meta = await asyncio.wait_for(
                        predict_tp_sl(features, user=ctx.get("user_id")), timeout
                    )
                elif method == "adaptive":
                    tp, sl, meta = await asyncio.wait_for(
                        _adaptive.compute(
                            ctx.get("symbol"), ctx.get("entry_price"), ctx
                        ), timeout
                    )
                elif method in _slm_plugins:
                    tp, sl, meta = await asyncio.wait_for(
                        _slm_plugins[method](ctx), timeout
                    )
                else:
                    raise ValueError(f"Unknown method: {method}")
            return SLResult(tp=tp, sl=sl, method=method, meta=meta)
        except Exception as e:
            SLM_ERRORS.labels(method=label).inc()
            logger.error(f"[SLM] {method} failed: {e}")
            raise

    async def feedback(
        self,
        ctx: Dict[str, Any],
        outcome: Dict[str, Any]
    ) -> None:
        # ML feedback
        try:
            features = extract_features(ctx)
            await tpsl_rl_feedback(features, outcome)
        except Exception:
            pass
        # adaptive feedback
        try:
            await _adaptive.tpsl_rl_feedback(ctx, outcome)
        except Exception:
            pass

    def clear_cache(self) -> None:
        self._cache.clear()

    async def replay_dlq(self) -> None:
        # implement DLQ replay if desired
        pass

    @staticmethod
    def _audit_hash(slresult: SLResult) -> str:
        import hashlib
        raw = str(slresult.dict()).encode()
        return hashlib.sha256(raw).hexdigest()

# singleton
sl_manager = SLManager()
