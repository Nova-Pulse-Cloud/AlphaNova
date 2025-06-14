 """
 Nova Pulse ALPHA — Reinforcement‑Learning Engine
 ------------------------------------------------
 * One entry‑point – `RLTrader` – used by sizing, router, and policy modules.
 * Strategy registry pattern with hot‑swap (`switch_policy`).
 * Prometheus counters + async event‑bus (no blocking in critical path).
 * Ready for real models (ONNX / tensor) but ships with fast heuristics until
   a live model is uploaded.
 * Zero placeholder data – every call wired to live context received from the
   Signal‑Engine.
 """

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from prometheus_client import Counter, Histogram

# ────────────────────────────────────────────────────────────────────────────────
# Logging & metrics
# ────────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("nova.rl_trader")
logger.setLevel(logging.INFO)

RL_ACTIONS = Counter("rl_actions_total", "Total RL actions taken")
RL_ERRORS = Counter("rl_errors_total", "Total RL errors")
RL_LATENCY = Histogram("rl_latency_seconds", "RL action latency")
RL_DLQ = Counter("rl_dlq_total", "RL DLQ events")
RL_POLICY_SWITCHES = Counter("rl_policy_switches_total", "Total policy switches")
RL_ACTION_AUDITS = Counter("rl_action_audits_total", "Total RL action audits")

# ────────────────────────────────────────────────────────────────────────────────
# Event‑bus helpers (non‑blocking async hooks)
# ────────────────────────────────────────────────────────────────────────────────
_rl_event_hooks: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def register_rl_event_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    _rl_event_hooks.append(hook_fn)

async def _fire_rl_event(evt: str, data: Dict[str, Any]):
    for h in _rl_event_hooks:
        try:
            await h(evt, data)
        except Exception as exc:  # noqa: BLE001 – want to see hook failures
            logger.error("[RL] hook %s failed: %s", h.__name__, exc)

# ────────────────────────────────────────────────────────────────────────────────
# Utility: audit hash for every action (trace‑able compliance)
# ────────────────────────────────────────────────────────────────────────────────

def _audit_hash(ctx: Dict[str, Any], action: Dict[str, Any]) -> str:
    blob = json.dumps(ctx, sort_keys=True) + json.dumps(action, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()

# ────────────────────────────────────────────────────────────────────────────────
# RLTrader – single façade used everywhere in codebase
# ────────────────────────────────────────────────────────────────────────────────
class RLTrader:
    """Lightweight strategy router.  Each *strategy* is an async callable that
    returns a dict::  {"action": "buy|sell|hold", "size": float}

    Strategies live in the registry `_strategies`; hot‑swappable at runtime for
    A/B testing or emergency fallbacks.
    """

    # class‑level registry so multiple instances share strategies
    _strategies: Dict[str, Callable[["RLTrader", Dict[str, Any]], Awaitable[Dict[str, Any]]]] = {}

    def __init__(self) -> None:
        self._loop = asyncio.get_event_loop()
        self._policy: str = "basic"  # default
        self._dlq: List[Dict[str, Any]] = []

    # ── public registration api ────────────────────────────────────────────
    @classmethod
    def register_strategy(cls, name: str):
        def _decorator(fn: Callable[["RLTrader", Dict[str, Any]], Awaitable[Dict[str, Any]]]):
            cls._strategies[name] = fn
            logger.info("[RL] strategy registered → %s", name)
            return fn
        return _decorator

    # ── core act() call – used by ExecutionRouter and sizing helpers ───────
    async def act(self, ctx: Dict[str, Any], *, user: Optional[str] = None, strategy: Optional[str] = None) -> Dict[str, Any]:
        RL_ACTIONS.inc()
        strat = strategy or self._policy
        fn = self._strategies.get(strat)
        if fn is None:
            logger.warning("[RL] unknown strategy %s – fallback to basic", strat)
            fn = self._strategies["basic"]
            strat = "basic"

        async with RL_LATENCY.time():
            try:
                action = await fn(self, ctx)  # type: ignore[arg-type]
                action.setdefault("action", "hold")
                action.setdefault("size", 0.0)
                audit = {"ctx": ctx, "action": action, "user": user, "strategy": strat, "audit_hash": _audit_hash(ctx, action)}
                RL_ACTION_AUDITS.inc()
                await _fire_rl_event("rl_action", audit)
                return action
            except Exception as exc:  # noqa: BLE001
                RL_ERRORS.inc(); RL_DLQ.inc()
                err_payload = {"ctx": ctx, "error": str(exc), "user": user}
                self._dlq.append(err_payload)
                await _fire_rl_event("rl_action_error", err_payload)
                logger.exception("[RL] act() error")
                return {"action": "hold", "size": 0.0, "error": str(exc)}

    # ── helpers for legacy code paths ───────────────────────────────────────
    async def get_position_size(self, symbol: str, user: Optional[str] = None, **extra) -> float:  # noqa: D401
        """Return *size* only (kept for backward compatibility)."""
        ctx = {"symbol": symbol, **extra}
        return float((await self.act(ctx, user=user)).get("size", 0.0))

    # ── runtime control ────────────────────────────────────────────────────
    async def switch_policy(self, new_policy: str):
        if new_policy not in self._strategies:
            raise ValueError(f"Unknown RL strategy: {new_policy}")
        self._policy = new_policy
        RL_POLICY_SWITCHES.inc()
        logger.info("[RL] ↪ policy switched → %s", new_policy)
        await _fire_rl_event("policy_switched", {"new_policy": new_policy})

    # ── DLQ exposure ───────────────────────────────────────────────────────
    def get_dlq(self) -> List[Dict[str, Any]]:
        return list(self._dlq)

# ────────────────────────────────────────────────────────────────────────────────
# Default *basic* strategy (risk‑aware heuristic)
# ────────────────────────────────────────────────────────────────────────────────
@RLTrader.register_strategy("basic")
async def _basic(trader: RLTrader, ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic heuristic: if signal score ≥0.8 → buy  ; ≤‑0.4 → sell  ; else hold.
    Size scales with volatility and confidence.
    """
    score = float(ctx.get("score", 0))
    vol = max(float(ctx.get("volatility", 0.02)), 1e-4)
    balance_norm = float(ctx.get("account_norm", 1))  # 0‑1 scaled account equity
    base_size = max(0.0, min(1.0, score)) / vol
    size = round(base_size * balance_norm, 4)

    if score >= 0.8:
        return {"action": "buy", "size": size}
    if score <= -0.4:
        return {"action": "sell", "size": size}
    return {"action": "hold", "size": 0.0}
