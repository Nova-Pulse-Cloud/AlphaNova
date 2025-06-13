import asyncio
import logging
import hashlib
import json
from typing import Dict, Any, Optional
from prometheus_client import Counter, Histogram

logger = logging.getLogger("reinforcement_learning")

# === Prometheus metrics ===
RL_ACTIONS = Counter("rl_actions_total", "Total RL actions taken")
RL_ERRORS = Counter("rl_errors_total", "Total RL errors")
RL_LATENCY = Histogram("rl_latency_seconds", "RL action latency")
RL_DLQ = Counter("rl_dlq_total", "RL DLQ events")
RL_POLICY_SWITCHES = Counter("rl_policy_switches_total", "Total policy switches")
RL_ACTION_AUDITS = Counter("rl_action_audits_total", "Total RL action audits")

# === DLQ, event hooks, plugin registry ===
FAILED_RL_DLQ = []
rl_event_hooks = []
def register_rl_event_hook(hook_fn):
    rl_event_hooks.append(hook_fn)

async def fire_rl_event(event_name, data):
    """
    Fires an async event hook to all listeners, such as for metrics and logging.
    """
    for hook in rl_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[Nova][RL] Event hook failed [{hook.__name__}]: {e}")

# === Audit hash ===
def rl_hash(ctx: dict, action: dict) -> str:
    """
    Generates a unique audit hash for each RL action based on the context and the action taken.
    """
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(action, sort_keys=True).encode())
    return m.hexdigest()

class RLTrader:
    def __init__(self):
        self.policies = {}
        self.rewards = {}  # user_id -> cumulative reward
        self.loop = asyncio.get_event_loop()
        self.policy_tracking = {}  # trace_id -> strategy

    async def connect(self):
        """
        Initialize the RL system. This could involve warming up models or other resources.
        """
        pass

    async def act(self, ctx: Dict[str, Any], strategy: str = "basic", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        """
        Determine the RL action to take based on the current signal context. The action may involve
        choosing whether to "buy", "sell", or "hold" a position, as well as determining the size.
        """
        RL_ACTIONS.inc()
        async with RL_LATENCY.time():
            try:
                # Dynamic strategy selection based on strategy name
                fn = rl_strategies.get(strategy, self.basic_policy)
                action = await fn(ctx, simulation=simulation)
                
                # Ensure the action always has 'action' and 'size' properties
                if "action" not in action:
                    action["action"] = "hold"
                if "size" not in action:
                    action["size"] = 0.0

                # Create an audit entry for tracking actions and metrics
                audit = {"ctx": ctx, "action": action, "user": user, "strategy": strategy}
                audit["audit_hash"] = rl_hash(ctx, action)

                trace_id = ctx.get("trace_id")
                if trace_id:
                    self.policy_tracking[trace_id] = strategy

                # Action audit and metrics tracking
                RL_ACTION_AUDITS.inc()
                await fire_rl_event("rl_action", audit)
                return action
            except Exception as e:
                RL_ERRORS.inc()
                RL_DLQ.inc()
                FAILED_RL_DLQ.append({"ctx": ctx, "error": str(e), "user": user})
                await fire_rl_event("rl_action_error", {"ctx": ctx, "error": str(e), "user": user})
                logger.error(f"[RLTrader] RL action error: {e}")
                return {"action": "hold", "size": 0.0, "error": str(e)}

    async def get_size(self, ctx: Dict[str, Any], **kwargs) -> float:
        """Return just the recommended trade size for legacy/plug use."""
        action = await self.act(ctx, **kwargs)
        return float(action.get("size", 0.0))

    async def get_action(self, ctx: Dict[str, Any], **kwargs) -> str:
        """Return just the recommended action (enter/exit/hold) for plug use."""
        action = await self.act(ctx, **kwargs)
        return action.get("action", "hold")

    async def switch_policy(self, new_policy: str, trace_id: str) -> None:
        """Switch RL strategy dynamically based on external conditions or market state."""
        self.policies[trace_id] = new_policy
        RL_POLICY_SWITCHES.inc()
        await fire_rl_event("policy_switched", {"trace_id": trace_id, "new_policy": new_policy})
        logger.info(f"Policy switched for trace {trace_id} to {new_policy}")
    
    # Example basic policy (to be replaced with real strategies)
    async def basic_policy(self, ctx: Dict[str, Any], simulation: bool = False) -> Dict[str, Any]:
        """
        A simple basic policy that returns "hold" with zero size by default.
        Can be extended or replaced with more advanced RL models or strategies.
        """
        action = "hold"
        size = 0.0
        return {"action": action, "size": size}

# === Strategy Registry ===
rl_strategies = {
    "basic": RLTrader.basic_policy,  # Placeholder strategy
    # Future strategies can be added here
}
