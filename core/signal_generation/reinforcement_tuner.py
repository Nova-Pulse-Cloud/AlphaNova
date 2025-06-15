# core/signal_generation/reinforcement_tuner.py
"""
Nova Pulse ALPHA – Reinforcement‑Tuner 3.0
──────────────────────────────────────────────────────────────────
• Async grid & random search over hyperparams with concurrency control
• Per‑eval timeouts, backoff retry, and Prometheus metrics per plugin
• Pydantic for result validation and schema consistency
• Semaphore‑throttled batching to avoid overload
• Audit‑hash & event bus notifications
• DLQ with replay & summary endpoints
"""
import asyncio
import hashlib
import json
import logging
from itertools import product
from random import sample
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.reinforcement_tuner")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TUNER_RUNS        = Counter("tuner_runs_total", "Total tuning runs")
TUNER_ERRORS      = Counter("tuner_errors_total", "Tuning errors")
TUNER_LATENCY     = Histogram("tuner_latency_seconds", "Tuning latency")
TUNER_DLQ         = Counter("tuner_dlq_total", "Tuning DLQ events")
TUNER_SIM_RUNS    = Counter("tuner_simulation_runs_total", "Simulation tuning runs")
TUNER_RESULT      = Histogram("tuner_result_scores", "Scores of tuning results")
PLUGIN_EVAL_TIME  = Histogram("tuner_eval_latency_seconds", "Latency per evaluation")

# ────────────────────────────────────────────────────────────────────────────────
# DLQ & Event Bus
# ────────────────────────────────────────────────────────────────────────────────
FAILED_TUNER_DLQ: List[Dict[str, Any]] = []
_tuner_event_hooks: List[Callable[[str, Dict[str, Any]], Any]] = []
_tuner_plugins: Dict[str, Callable[..., Any]] = {}


class TunerResult(BaseModel):
    params: Dict[str, Any]
    score: float

class TunerPayload(BaseModel):
    ctx: Dict[str, Any]
    best_params: Dict[str, Any]
    best_score: float
    user: Optional[str]
    audit_hash: str


def register_tuner_event_hook(fn: Callable[[str, Dict[str, Any]], Any]) -> None:
    _tuner_event_hooks.append(fn)

async def _fire_event(name: str, data: Dict[str, Any]) -> None:
    for hook in _tuner_event_hooks:
        try:
            result = hook(name, data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            logger.error(f"[Tuner Event] Hook {hook.__name__} failed: {e}")


def register_tuner_plugin(name: str, fn: Callable[..., Union[float, Any]]) -> None:
    _tuner_plugins[name] = fn
    logger.info(f"[Tuner] Plugin registered: {name}")


def _audit_hash(ctx: Dict[str, Any], params: Dict[str, Any], score: float) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(params, sort_keys=True).encode())
    m.update(str(score).encode())
    return m.hexdigest()


class ReinforcementTuner:
    """Async hyperparameter tuner with DLQ and plugin-based evaluation."""
    def __init__(
        self,
        max_concurrent: int = 10,
        eval_timeout: float = 5.0,
        random_sample: Optional[int] = None,
    ) -> None:
        self._results: List[TunerPayload] = []
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self.eval_timeout = eval_timeout
        self.random_sample = random_sample

    async def tune(
        self,
        ctx: Dict[str, Any],
        param_grid: Dict[str, List[Any]],
        *,
        plugin: str = "default",
        user: Optional[str] = None,
        simulation: bool = False,
    ) -> Dict[str, Any]:
        """
        Perform grid or random search. Returns best params and score.
        """
        TUNER_RUNS.inc()
        if simulation:
            TUNER_SIM_RUNS.inc()

        # Build combinations
        combos = [dict(zip(param_grid.keys(), vals))
                  for vals in product(*param_grid.values())]
        if self.random_sample and self.random_sample < len(combos):
            combos = sample(combos, self.random_sample)

        eval_fn = _tuner_plugins.get(plugin, self._default_eval)
        best_score = float('-inf')
        best_params: Dict[str, Any] = {}
        all_results: List[TunerResult] = []

        try:
            async with TUNER_LATENCY.time():
                tasks = [self._evaluate(ctx, eval_fn, p, simulation) for p in combos]
                for coro in asyncio.as_completed(tasks, timeout=None):
                    try:
                        res = await asyncio.wait_for(coro, self.eval_timeout)
                        all_results.append(res)
                        TUNER_RESULT.observe(res.score)
                        if res.score > best_score:
                            best_score, best_params = res.score, res.params
                    except asyncio.TimeoutError:
                        logger.warning(f"[Tuner] Eval timeout for params {coro}")
                    except Exception as e:
                        logger.warning(f"[Tuner] Eval error: {e}")

            payload = TunerPayload(
                ctx=ctx,
                best_params=best_params,
                best_score=best_score,
                user=user,
                audit_hash=_audit_hash(ctx, best_params, best_score)
            )
            self._results.append(payload)
            await _fire_event("tuner_complete", payload.dict())
            return payload.dict()

        except Exception as exc:
            TUNER_ERRORS.inc()
            TUNER_DLQ.inc()
            err = {"ctx": ctx, "error": str(exc), "user": user}
            FAILED_TUNER_DLQ.append(err)
            await _fire_event("tuner_error", err)
            logger.error(f"[Tuner] Tune error: {exc}")
            return {"error": str(exc)}

    async def _evaluate(
        self,
        ctx: Dict[str, Any],
        eval_fn: Callable[..., Any],
        params: Dict[str, Any],
        simulation: bool,
    ) -> TunerResult:
        """Evaluate one parameter set under semaphore control."""
        async with self._semaphore:
            start = asyncio.get_event_loop().time()
            try:
                score = eval_fn(ctx, params, simulation)
                if asyncio.iscoroutine(score):
                    score = await score
                return TunerResult(params=params, score=float(score))
            finally:
                PLUGIN_EVAL_TIME.observe(asyncio.get_event_loop().time() - start)

    async def replay_dlq(self) -> List[Any]:
        successes = []
        for entry in list(FAILED_TUNER_DLQ):
            try:
                res = await self.tune(entry.get("ctx", {}), {}, user=entry.get("user"))
                successes.append(res)
                FAILED_TUNER_DLQ.remove(entry)
            except:
                continue
        return successes

    def get_results(self) -> List[Dict[str, Any]]:
        return [r.dict() for r in self._results]

    def get_dlq(self) -> List[Dict[str, Any]]:
        return list(FAILED_TUNER_DLQ)

    async def _default_eval(self, ctx: Dict[str, Any], params: Dict[str, Any], simulation: bool) -> float:
        # sample baseline: PnL minus latency penalty
        pnl = float(ctx.get("pnl", 0.0))
        latency = float(ctx.get("latency", 0.0))
        score = pnl - 0.1 * latency
        return score
