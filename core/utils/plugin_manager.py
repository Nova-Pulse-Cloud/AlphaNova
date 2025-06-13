import importlib
import logging
import hashlib
import asyncio
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram
import uuid
from aiocache import Cache

# === Prometheus metrics ===
PLUGIN_LOADS = Counter("plugin_loads_total", "Total plugins loaded")
PLUGIN_ERRORS = Counter("plugin_errors_total", "Plugin load/runtime errors")
PLUGIN_LATENCY = Histogram("plugin_latency_seconds", "Plugin execution latency")
PLUGIN_DLQ = Counter("plugin_dlq_total", "Plugin DLQ events")

FAILED_PLUGIN_DLQ = []
plugin_event_hooks: List[Callable] = []

# === Cache Setup ===
cache = Cache.from_url("redis://localhost:6379")
cache.namespace = "nova_plugins"  # Setting the namespace after initializing the Cache

def register_plugin_event_hook(hook_fn: Callable):
    plugin_event_hooks.append(hook_fn)

async def fire_plugin_event(event_name: str, data: dict):
    for hook in plugin_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            log.warning(f"[Nova][PluginManager] Event hook failed: {e}")

log = logging.getLogger("nova.plugin_manager")
log.setLevel(logging.WARNING)  # Set the log level to WARNING to avoid unnecessary log flooding

class PluginManager:
    """
    Nova Pulse ALPHA: Full async mesh plugin manager—router, trade hooks, health, meta, confidence.
    """
    def __init__(self):
        self.plugins: Dict[str, Any] = {}
        self.plugin_meta: Dict[str, dict] = {}
        self.plugin_health: Dict[str, str] = {}
        self.plugin_performance: Dict[str, Dict[str, dict]] = {}
        self.plugins_by_type: Dict[str, List[str]] = {}
        self.loop = asyncio.get_event_loop()

    async def initialize(self, plugins: Optional[Dict[str, str]] = None):
        """
        Initialize all plugins from dict: {name: module_path}.
        """
        if plugins:
            for name, module_path in plugins.items():
                await self.load_plugin(name, module_path)

    async def load_plugin(self, name: str, module_path: str):
        """
        Load (or reload) a plugin module.
        """
        try:
            PLUGIN_LOADS.inc()
            async with PLUGIN_LATENCY.time():
                mod = importlib.import_module(module_path)
                self.plugins[name] = mod
                meta = {
                    "module": module_path,
                    "version": getattr(mod, "__version__", "0.1.0"),
                    "type": getattr(mod, "__type__", "unknown"),
                    "capabilities": getattr(mod, "__capabilities__", []),
                    "modes": getattr(mod, "__modes__", ["live", "sim"]),
                    "confidence": 0.5
                }
                self.plugin_meta[name] = meta
                self.plugin_health[name] = "healthy"
                self.plugins_by_type.setdefault(meta["type"], []).append(name)
                self.plugin_performance[name] = {}

                if hasattr(mod, "__on_load__"):
                    await mod.__on_load__()

                await fire_plugin_event("plugin_loaded", {"name": name, "module": module_path})
                log.info(f"[Nova][PluginManager] Loaded plugin: {name} from {module_path}")

        except Exception as e:
            PLUGIN_ERRORS.inc()
            PLUGIN_DLQ.inc()
            FAILED_PLUGIN_DLQ.append({"name": name, "module": module_path, "error": str(e)})
            self.plugin_health[name] = "error"
            await fire_plugin_event("plugin_load_failed", {"name": name, "module": module_path, "error": str(e)})
            log.error(f"[Nova][PluginManager] Plugin load failed: {name} from {module_path}: {e}")

    async def reload_plugin(self, name: str):
        """
        Reload a plugin in place.
        """
        if name in self.plugins:
            path = self.plugins[name].__name__
            del self.plugins[name]
            await self.load_plugin(name, path)

    async def plugin_health_check(self):
        """
        Health check for all loaded plugins (calls .health() if present).
        """
        for name in self.plugins:
            healthy = True
            try:
                getattr(self.plugins[name], "health", lambda: True)()
            except Exception:
                healthy = False
            self.plugin_health[name] = "healthy" if healthy else "error"
            await fire_plugin_event("plugin_health", {"name": name, "healthy": healthy})

    async def pre_trade(self, signal: Dict[str, Any]):
        """
        Run all plugins with 'pre_trade' capability before trade execution.
        """
        for name, mod in self.plugins.items():
            if "pre_trade" in self.plugin_meta.get(name, {}).get("capabilities", []):
                try:
                    signal.setdefault("trace_id", f"plugin-{uuid.uuid4().hex[:6]}")
                    async with PLUGIN_LATENCY.time():
                        await mod.pre_trade(signal)
                except Exception as e:
                    PLUGIN_ERRORS.inc()
                    await fire_plugin_event("plugin_pre_trade_error", {"name": name, "error": str(e), "trace_id": signal.get("trace_id")})

    async def post_trade(self, signal: Dict[str, Any], tx_result: Dict[str, Any]):
        """
        Run all plugins with 'post_trade' capability after trade execution.
        """
        for name, mod in self.plugins.items():
            if "post_trade" in self.plugin_meta.get(name, {}).get("capabilities", []):
                try:
                    async with PLUGIN_LATENCY.time():
                        await mod.post_trade(signal, tx_result)
                except Exception as e:
                    PLUGIN_ERRORS.inc()
                    await fire_plugin_event("plugin_post_trade_error", {"name": name, "error": str(e), "trace_id": signal.get("trace_id")})

    async def plugin_feedback(self, name: str, coin: str, pnl: float, sl_hit: bool, trace_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None):
        """
        Log plugin PnL feedback and adjust confidence.
        """
        trace = trace_id or f"trace-{uuid.uuid4().hex[:8]}"
        perf = self.plugin_performance.setdefault(name, {}).setdefault(coin, {"trades": 0, "pnl": 0, "sl_hits": 0})
        perf["trades"] += 1
        perf["pnl"] += pnl
        if sl_hit:
            perf["sl_hits"] += 1
        delta = 0.01 if pnl > 0 else (-0.03 if sl_hit else -0.01)
        confidence = self.plugin_meta[name]["confidence"]
        self.plugin_meta[name]["confidence"] = max(0.0, min(1.0, confidence + delta))
        await fire_plugin_event("plugin_feedback", {
            "name": name,
            "coin": coin,
            "pnl": pnl,
            "sl_hit": sl_hit,
            "confidence": self.plugin_meta[name]["confidence"],
            "trace_id": trace,
            "meta": metadata or {}
        })

    async def plugin_signal(self, name: str, symbol: str, strength: float, strategy: str, trace_id: Optional[str] = None, meta: Optional[Dict[str, Any]] = None):
        """
        Fire a plugin-generated signal (for mesh/event/ops).
        """
        trace = trace_id or f"trace-{uuid.uuid4().hex[:8]}"
        await fire_plugin_event("plugin_signal", {
            "plugin": name,
            "symbol": symbol,
            "strength": strength,
            "strategy": strategy,
            "trace_id": trace,
            "meta": meta or {}
        })

    def get_plugin(self, name: str, default: Any = None):
        return self.plugins.get(name, default)

    def get_router(self, venue: str) -> Optional[Callable]:
        for name, mod in self.plugins.items():
            if getattr(mod, "__type__", None) == "router" and hasattr(mod, "route"):
                if venue in getattr(mod, "__venues__", [venue]):
                    return mod.route
        return None

    def plugin_audit_hash(self, name: str) -> str:
        meta = self.plugin_meta.get(name, {})
        m = hashlib.sha256()
        m.update(str(meta).encode())
        return m.hexdigest()

    def get_plugin_health(self):
        return self.plugin_health.copy()

    def get_dlq(self):
        return FAILED_PLUGIN_DLQ.copy()

    def get_plugin_meta(self):
        return self.plugin_meta.copy()

    def list_plugins_by_type(self, type_filter: Optional[str] = None):
        if type_filter:
            return self.plugins_by_type.get(type_filter, [])
        return self.plugins_by_type.copy()
