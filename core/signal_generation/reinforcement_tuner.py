import asyncio
import logging
import hashlib
import json
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram
import time
import random

logger = logging.getLogger("reinforcement_tuner")

# === Prometheus metrics ===
TUNER_RUNS = Counter("tuner_runs_total", "Total RL tuner runs")
TUNER_ERRORS = Counter("tuner_errors_total", "RL tuner errors")
TUNER_LATENCY = Histogram("tuner_latency_seconds", "RL tuner latency")
TUNER_DLQ = Counter("tuner_dlq_total", "RL tuner DLQ events")
TUNER_SIM = Counter("tuner_sim_total", "RL tuner simulation runs")
TUNER_SCORE = Counter("tuner_score_total", "RL tuner scores")

# === DLQ, event hooks, plugin registry ===
FAILED_TUNER_DLQ = []
tuner_event_hooks = []
def register_tuner_event_hook(hook_fn):
    tuner_event_hooks.append(hook_fn)
async def fire_tuner_event(event_name, data):
    for hook in tuner_event_hooks:
        await hook(event_name, data)

tuner_plugins = {}
def register_tuner_plugin(name: str, fn: Callable):
    tuner_plugins[name] = fn

# === Audit hash ===
def tuner_hash(ctx: dict, params: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(params, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class ReinforcementTuner:
    def __init__(self):
        self.params = {}
        self.results = []
        self.loop = asyncio.get_event_loop()

    async def tune(self, ctx: Dict[str, Any], param_grid: Dict[str, List[Any]], plugin: str = "default", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        TUNER_RUNS.inc()
        best_params, best_score = None, float('-inf')
        all_results = []
        try:
            async with TUNER_LATENCY.time():
                param_keys = list(param_grid.keys())
                combos = self._grid_combos(param_grid)

                # Parallel evaluation of parameter combinations for faster tuning
                tasks = [self._evaluate(ctx, params, plugin, user, simulation) for params in combos]
                results = await asyncio.gather(*tasks)

                for result in results:
                    all_results.append(result)
                    if result["score"] > best_score:
                        best_score, best_params = result["score"], result["params"]
                
                audit = {"ctx": ctx, "best_params": best_params, "best_score": best_score, "user": user}
                audit["audit_hash"] = tuner_hash(ctx, best_params or {}, {"score": best_score})
                await fire_tuner_event("tune_complete", audit)
                self.results.append(audit)
                if simulation:
                    TUNER_SIM.inc()
                    await fire_tuner_event("tune_simulation", audit)
                
                TUNER_SCORE.inc()  # Increment the score metric
                return audit
        except Exception as e:
            TUNER_ERRORS.inc()
            TUNER_DLQ.inc()
            FAILED_TUNER_DLQ.append({"ctx": ctx, "error": str(e), "user": user})
            await fire_tuner_event("tune_error", {"ctx": ctx, "error": str(e), "user": user})
            logger.error(f"[ReinforcementTuner] RL tuning error: {e}")
            return {"error": str(e), "user": user}

    async def _evaluate(self, ctx: Dict[str, Any], params: Dict[str, Any], plugin: str, user: Optional[str], simulation: bool) -> Dict:
        eval_fn = tuner_plugins.get(plugin, self.default_eval)
        score = await eval_fn(ctx, params, user=user, simulation=simulation)
        return {"params": params, "score": score, "user": user}

    async def default_eval(self, ctx: Dict[str, Any], params: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> float:
        pnl = float(ctx.get("pnl", 0.0))
        sl_hit = ctx.get("sl_hit", False)
        latency = float(ctx.get("latency", 0.0))
        volatility = float(ctx.get("volatility", 0.0))
        confidence = float(ctx.get("confidence", 0.5))

        reward = 0.0
        reward += pnl * 2.0
        reward -= latency * 0.5
        reward -= volatility * 0.1
        reward += (confidence - 0.5)
        if sl_hit:
            reward -= 1.5

        if simulation:
            reward += 0.1  # Encourage exploration

        return reward

    def _grid_combos(self, grid: Dict[str, List[Any]]):
        from itertools import product
        keys = list(grid.keys())
        vals = [grid[k] for k in keys]
        for prod in product(*vals):
            yield dict(zip(keys, prod))

    async def hot_reload_plugin(self, name: str, fn: Callable):
        tuner_plugins[name] = fn
        await fire_tuner_event("tuner_hot_reload", {"name": name})

    async def replay_dlq(self):
        for failed in FAILED_TUNER_DLQ[:]:
            try:
                _ = await self.tune(failed.get("ctx", {}), {}, user=failed.get("user"))
                FAILED_TUNER_DLQ.remove(failed)
            except Exception as e:
                continue

    def get_dlq(self):
        return FAILED_TUNER_DLQ.copy()

    def get_results(self):
        return self.results
