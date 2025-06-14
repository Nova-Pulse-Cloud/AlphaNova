"""
Nova Pulse ALPHA – Reinforcement‑Tuner 2.0
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
* Grid / random search over hyper‑params
* Fully async with batch evaluation & Prometheus hooks
* Hot‑reloadable plugin registry
* Audit‑hashed results → event bus → downstream dashboards
* No mocks – designed to be driven by **live** ctx dictionaries coming from
  the Execution‑Router / Alpha‑Signal‑Agent.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from itertools import product
from typing import Any, Callable, Dict, List, Optional

from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.reinforcement_tuner")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
TUNER_RUNS           = Counter("tuner_runs_total", "Total RL‑tuner runs")
TUNER_ERRORS         = Counter("tuner_errors_total", "RL‑tuner errors")
TUNER_LATENCY        = Histogram("tuner_latency_seconds", "RL‑tuner latency")
TUNER_DLQ            = Counter("tuner_dlq_total", "RL‑tuner DLQ events")
TUNER_SIM_RUNS       = Counter("tuner_sim_total", "Simulation tuning passes")
TUNER_BEST_SCORE     = Histogram("tuner_best_score", "Best score per tuning batch")

# ────────────────────────────────────────────────────────────────────────────────
# Event‑bus & DLQ
# ────────────────────────────────────────────────────────────────────────────────
FAILED_TUNER_DLQ: list[Dict[str, Any]] = []
_tuner_event_hooks: list[Callable[[str, Dict[str, Any]], Any]] = []


def register_tuner_event_hook(hook_fn: Callable[[str, Dict[str, Any]], Any]):
    _tuner_event_hooks.append(hook_fn)


async def _fire_event(name: str, data: Dict[str, Any]):
    for hook in _tuner_event_hooks:
        try:
            maybe_coroutine = hook(name, data)
            if asyncio.iscoroutine(maybe_coroutine):
                await maybe_coroutine
        except Exception as e:  # pragma: no cover – defensive
            logger.error("[Tuner Event] Hook %s failed: %s", hook.__name__, e)

# ────────────────────────────────────────────────────────────────────────────────
# Plugin Registry
# ────────────────────────────────────────────────────────────────────────────────
_tuner_plugins: Dict[str, Callable[[Dict[str, Any], Dict[str, Any]], Any]] = {}

def register_tuner_plugin(name: str, fn: Callable[[Dict[str, Any], Dict[str, Any]], Any]):
    _tuner_plugins[name] = fn
    logger.info("[Tuner] Plugin %s registered", name)

# ────────────────────────────────────────────────────────────────────────────────
# Audit‑hash helper
# ────────────────────────────────────────────────────────────────────────────────

def _audit_hash(ctx: Dict[str, Any], params: Dict[str, Any], score: float) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(params, sort_keys=True).encode())
    m.update(str(score).encode())
    return m.hexdigest()

# ────────────────────────────────────────────────────────────────────────────────
# Core Class
# ────────────────────────────────────────────────────────────────────────────────

class ReinforcementTuner:
    """Hyper‑parameter tuner for RL components (sizing, TP/SL, node‑picker, …)."""

    def __init__(self):
        self._results: list[Dict[str, Any]] = []
        self._loop = asyncio.get_event_loop()

    # ‑‑ public api ‑‑
    async def tune(
        self,
        ctx: Dict[str, Any],
        param_grid: Dict[str, List[Any]],
        *,
        plugin: str = "default",
        user: Optional[str] = None,
        simulation: bool = False,
    ) -> Dict[str, Any]:
        """Exhaustive grid‑search (async) over *param_grid* using *plugin* eval fn."""

        TUNER_RUNS.inc()
        if simulation:
            TUNER_SIM_RUNS.inc()

        best_params: Dict[str, Any] | None = None
        best_score: float = float("-inf")
        all_results: list[Dict[str, Any]] = []

        eval_fn = _tuner_plugins.get(plugin, self._default_eval)

        try:
            async with TUNER_LATENCY.time():
                combos = (dict(zip(param_grid.keys(), prod)) for prod in product(*param_grid.values()))
                tasks = [self._evaluate(eval_fn, ctx, p, simulation) for p in combos]
                for res in await asyncio.gather(*tasks):
                    all_results.append(res)
                    if res["score"] > best_score:
                        best_score, best_params = res["score"], res["params"]

            payload = {
                "ctx": ctx,
                "best_params": best_params,
                "best_score": best_score,
                "user": user,
                "audit_hash": _audit_hash(ctx, best_params or {}, best_score),
            }
            TUNER_BEST_SCORE.observe(best_score)
            self._results.append(payload)
            await _fire_event("tuner_complete", payload)
            return payload

        except Exception as exc:
            TUNER_ERRORS.inc()
            TUNER_DLQ.inc()
            err_payload = {"ctx": ctx, "error": str(exc), "user": user}
            FAILED_TUNER_DLQ.append(err_payload)
            await _fire_event("tuner_error", err_payload)
            logger.exception("[Tuner] Error during tuning")
            return {"error": str(exc)}

    # ‑‑ DLQ helpers ‑‑
    async def replay_dlq(self):
        for item in list(FAILED_TUNER_DLQ):
            try:
                await self.tune(item["ctx"], {}, user=item.get("user"))
                FAILED_TUNER_DLQ.remove(item)
            except Exception:  # keep in DLQ
                continue

    def get_dlq(self):
        return list(FAILED_TUNER_DLQ)

    def get_results(self):
        return list(self._results)

    # ‑‑ private helpers ‑‑
    async def _evaluate(
        self,
        eval_fn: Callable[[Dict[str, Any], Dict[str, Any], bool], Any],
        ctx: Dict[str, Any],
        params: Dict[str, Any],
        simulation: bool,
    ) -> Dict[str, Any]:
        score = await eval_fn(ctx, params, simulation)
        return {"params": params, "score": float(score)}

    # ⇢ baseline evaluation (override with plugin)
    async def _default_eval(self, ctx: Dict[str, Any], params: Dict[str, Any], simulation: bool = False) -> float:  # noqa: D401
        """Reward = weighted function of PnL / latency / volatility / SL‑hit."""
        pnl        = float(ctx.get("pnl", 0.0))
        sl_hit     = ctx.get("sl_hit", False)
        latency    = float(ctx.get("latency", 0.0))
        volatility = float(ctx.get("volatility", 0.0))
        confidence = float(ctx.get("confidence", 0.5))

        reward = (pnl * 2.0) - (latency * 0.5) - (volatility * 0.1) + (confidence - 0.5)
        if sl_hit:
            reward -= 1.5
        if simulation:
            reward += 0.1  # encourage exploration
        return reward
