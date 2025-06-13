import asyncio
import logging
import hashlib
import json
import uuid
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram

logger = logging.getLogger("rotation_sentinel")

# === Prometheus metrics ===
ROTATE_RUNS = Counter("rotation_runs_total", "Total portfolio rotation checks")
ROTATE_ERRORS = Counter("rotation_errors_total", "Rotation errors")
ROTATE_LATENCY = Histogram("rotation_latency_seconds", "Rotation latency")
ROTATE_DLQ = Counter("rotation_dlq_total", "Rotation DLQ events")
ROTATE_ALERTS = Counter("rotation_alert_total", "Rotation alert triggers")
ROTATE_PNL_ALERTS = Counter("rotation_pnl_alerts_total", "PnL-based rotation alerts")

# === DLQ, event hooks, plugin registry ===
FAILED_ROTATE_DLQ = []
rotation_event_hooks = []

def register_rotation_event_hook(hook_fn):
    rotation_event_hooks.append(hook_fn)

async def fire_rotation_event(event_name, data):
    for hook in rotation_event_hooks:
        await hook(event_name, data)

rotation_plugins = {}
def register_rotation_plugin(name: str, fn: Callable):
    rotation_plugins[name] = fn

# === Audit hash ===
def rotation_hash(ctx: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(json.dumps(ctx, sort_keys=True).encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class RotationSentinel:
    def __init__(self):
        self.last_rotation = None
        self.alerts = []
        self.loop = asyncio.get_event_loop()
        self.cache = {}

    async def check_rotation(self, ctx: Dict[str, Any], plugin: str = "default", user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        """
        Main entry: run portfolio rotation check with RL/plugin, alert, audit, trace_id on every run.
        """
        ROTATE_RUNS.inc()
        trace_id = f"rotate-{uuid.uuid4().hex[:8]}"
        try:
            async with ROTATE_LATENCY.time():
                # Plugin/ensemble logic for advanced meta rotation
                fn = rotation_plugins.get(plugin, self.default_rotation)
                rotation = await fn(ctx, user=user, simulation=simulation)
                rationale = rotation.get("reason", "default rationale")
                audit = {
                    "ctx": ctx, "rotation": rotation, "user": user,
                    "simulation": simulation, "trace_id": trace_id, "rationale": rationale
                }
                audit["audit_hash"] = rotation_hash(ctx, rotation)
                await fire_rotation_event("rotation_checked", audit)
                self.last_rotation = audit
                if rotation.get("alert"):
                    ROTATE_ALERTS.inc()
                    ROTATE_PNL_ALERTS.inc()
                    self.alerts.append(audit)
                    await fire_rotation_event("rotation_alert", audit)
                return audit
        except Exception as e:
            ROTATE_ERRORS.inc()
            ROTATE_DLQ.inc()
            FAILED_ROTATE_DLQ.append({"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id})
            await fire_rotation_event("rotation_error", {"ctx": ctx, "error": str(e), "user": user, "trace_id": trace_id})
            logger.error(f"[RotationSentinel] Rotation check error: {e}")
            return {"error": str(e), "user": user, "trace_id": trace_id}

    async def default_rotation(self, ctx: Dict[str, Any], user: Optional[str] = None, simulation: bool = False) -> Dict[str, Any]:
        """
        Example: Check PnL, time, or external signals; trigger alert on thresholds.
        Enhanced to integrate with RL and advanced metrics.
        """
        pnl = ctx.get("pnl", 0.0)
        cycles = ctx.get("cycles", 0)
        should_rotate = pnl < -0.10 or cycles > 7  # Condition can be enhanced
        reason = "PnL drop or cycle count exceeded"
        
        if should_rotate:
            # Add additional context information for better decision making
            reason += f" | PnL: {pnl} | Cycles: {cycles}"

        return {
            "should_rotate": should_rotate,
            "alert": should_rotate,
            "reason": reason,
            "target": ctx.get("target", []),
            "timestamp": ctx.get("timestamp"),
            "simulation": simulation,
        }

    async def replay_dlq(self):
        for failed in FAILED_ROTATE_DLQ[:]:
            try:
                _ = await self.check_rotation(failed.get("ctx", {}), user=failed.get("user"))
                FAILED_ROTATE_DLQ.remove(failed)
            except Exception as e:
                continue

    def get_dlq(self):
        return FAILED_ROTATE_DLQ.copy()

    def get_alerts(self):
        return self.alerts

    def get_last_rotation(self):
        return self.last_rotation
