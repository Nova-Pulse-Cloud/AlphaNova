# core/signal_generation/reinforcement_learning.py
"""
Nova Pulse ALPHA — Reinforcement-Learning Engine 2.0
───────────────────────────────────────────────────────────────
• Single async façade `RLTrader.act` with per-strategy timeouts
• Pydantic validation of strategy outputs for safety
• Prometheus metrics labeled by strategy: actions, errors, latency
• Async event-bus notifications without blocking exec path
• Hot-swappable strategy registry with policy switches
• DLQ capture & replay for failed actions
"""
import asyncio
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable, Dict, List, Optional

from pydantic import BaseModel, Field, ValidationError
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.rl_trader")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
RL_ACTIONS = Counter(
    "rl_actions_total", "Total RL actions taken", ["strategy"]
)
RL_ERRORS = Counter(
    "rl_errors_total",  "Total RL errors", ["strategy"]
)
RL_LATENCY = Histogram(
    "rl_latency_seconds", "Latency of RL strategy execution", ["strategy"]
)
RL_DLQ = Counter(
    "rl_dlq_total", "RL DLQ events", ["strategy"]
)
RL_POLICY_SWITCHES = Counter(
    "rl_policy_switches_total", "Total policy switches"
)

# ────────────────────────────────────────────────────────────────────────────────
# Event Bus
# ────────────────────────────────────────────────────────────────────────────────
_rl_event_hooks: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def register_rl_event_hook(fn: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> None:
    _rl_event_hooks.append(fn)

async def _fire_rl_event(event: str, payload: Dict[str, Any]) -> None:
    for hook in _rl_event_hooks:
        try:
            coro = hook(event, payload)
            if asyncio.iscoroutine(coro):
                await coro
        except Exception as e:
            logger.error(f"[RL] Event hook {hook.__name__} failed: {e}")

# ────────────────────────────────────────────────────────────────────────────────
# Data Models
# ────────────────────────────────────────────────────────────────────────────────
class RLAction(BaseModel):
    action: str = Field(..., description="buy, sell, or hold")
    size: float = Field(..., ge=0, description="Position size fraction or units")

class RLAudit(BaseModel):
    strategy: str
    ctx: Dict[str, Any]
    action: RLAction
    user: Optional[str]
    audit_hash: str

# ────────────────────────────────────────────────────────────────────────────────
# RLTrader Implementation
# ────────────────────────────────────────────────────────────────────────────────
class RLTrader:
    _strategies: Dict[
        str,
        Callable[["RLTrader", Dict[str, Any]], Awaitable[Dict[str, Any]]]
    ] = {}

    def __init__(self) -> None:
        self._policy: str = "basic"
        self._dlq: List[Dict[str, Any]] = []

    @classmethod
    def register_strategy(cls, name: str) -> Callable:
        def decorator(fn: Callable[["RLTrader", Dict[str, Any]], Awaitable[Dict[str, Any]]]):
            cls._strategies[name] = fn
            logger.info(f"[RL] Strategy registered → {name}")
            return fn
        return decorator

    async def act(
        self,
        ctx: Dict[str, Any],
        user: Optional[str] = None,
        strategy: Optional[str] = None,
    ) -> RLAction:
        strat = strategy or self._policy
        fn = self._strategies.get(strat)
        if fn is None:
            logger.warning(f"[RL] Unknown strategy '{strat}', falling back to 'basic'")
            strat = "basic"
            fn = self._strategies.get(strat)

        start = asyncio.get_event_loop().time()
        try:
            async with RL_LATENCY.labels(strategy=strat).time():
                raw = await asyncio.wait_for(
                    fn(self, ctx), timeout=getattr(config, "RL_ACTION_TIMEOUT", 3.0)
                )

            # Validate output
            try:
                action = RLAction(**raw)
            except ValidationError as ve:
                raise RuntimeError(f"Invalid action from '{strat}': {ve}")

            # Record metrics & audit
            RL_ACTIONS.labels(strategy=strat).inc()
            audit_hash = hashlib.sha256(
                (json.dumps(ctx, sort_keys=True) + action.json()).encode()
            ).hexdigest()
            audit = RLAudit(
                strategy=strat,
                ctx=ctx,
                action=action,
                user=user,
                audit_hash=audit_hash,
            )
            await _fire_rl_event("rl_action", audit.dict())
            return action

        except Exception as exc:
            # Log error & enqueue DLQ
            RL_ERRORS.labels(strategy=strat).inc()
            RL_DLQ.labels(strategy=strat).inc()
            err = {"strategy": strat, "ctx": ctx, "error": str(exc), "user": user}
            self._dlq.append(err)
            await _fire_rl_event("rl_action_error", err)
            logger.exception(f"[RL] act() error in strategy '{strat}'")
            return RLAction(action="hold", size=0.0)

    async def switch_policy(self, new_policy: str) -> None:
        if new_policy not in self._strategies:
            raise ValueError(f"Unknown policy '{new_policy}'")
        self._policy = new_policy
        RL_POLICY_SWITCHES.inc()
        logger.info(f"[RL] Policy switched → {new_policy}")
        await _fire_rl_event("policy_switched", {"new_policy": new_policy})

    def get_dlq(self) -> List[Dict[str, Any]]:
        return self._dlq.copy()

# ────────────────────────────────────────────────────────────────────────────────
# Default Basic Strategy
# ────────────────────────────────────────────────────────────────────────────────
@RLTrader.register_strategy("basic")
async def _basic_strategy(trader: RLTrader, ctx: Dict[str, Any]) -> Dict[str, Any]:
    score = float(ctx.get("score", 0))
    vol   = max(float(ctx.get("volatility", 1e-4)), 1e-4)
    base  = max(0.0, min(1.0, score)) / vol
    size  = round(base * float(ctx.get("account_norm", 1.0)), 4)

    if score >= 0.8:
        return {"action": "buy",  "size": size}
    if score <= -0.4:
        return {"action": "sell", "size": size}
    return {"action": "hold", "size": 0.0}

# Alias for backward-compatibility / CombinedSignalEngine
DQNAgent = RLTrader
