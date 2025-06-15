# core/utils/plugin_manager.py

import asyncio
import importlib
import inspect
import logging
import pkgutil
from typing import Any, Awaitable, Callable, Dict, List, Optional

from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.plugin_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
PLUG_LOADS = Counter(
    "plugin_loads_total", "Number of plugins successfully loaded", ["plugin"]
)
PLUG_ERRORS = Counter(
    "plugin_errors_total", "Number of plugin import or runtime errors", ["plugin"]
)
PLUG_LATENCY = Histogram(
    "plugin_latency_seconds", "Latency of plugin hook execution", ["plugin","stage"]
)

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue for failed plugin operations
# ────────────────────────────────────────────────────────────────────────────────
FAILED_PLUGIN_DLQ: List[Dict[str, Any]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Global event hooks bus
# ────────────────────────────────────────────────────────────────────────────────
_EVENT_HOOKS: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

def fire_plugin_event(name: str, payload: Dict[str, Any]) -> Awaitable[None]:
    """Dispatch an event to all registered plugin event hooks."""
    async def _dispatch():
        for hook in list(_EVENT_HOOKS):
            try:
                with PLUG_LATENCY.labels(plugin=hook.__name__, stage="event").time():
                    await hook(name, payload)
            except Exception as exc:
                logger.error(f"[PluginManager] Event hook {hook.__name__} failed: {exc}")
                PLUG_ERRORS.labels(plugin=hook.__name__).inc()
                FAILED_PLUGIN_DLQ.append({"hook": hook.__name__, "event": name, "error": str(exc)})
    return _dispatch()


def register_plugin_event_hook(fn: Callable[[str, Dict[str, Any]], Awaitable[None]]) -> None:
    """Register a coroutine to receive plugin events."""
    _EVENT_HOOKS.append(fn)

# ────────────────────────────────────────────────────────────────────────────────
# PluginManager Core
# ────────────────────────────────────────────────────────────────────────────────
class PluginManager:
    """
    Discovers, loads, and manages plugin hooks: router, pre_trade, post_trade, and register.
    Provides hot-reload, thread-safety, event bus, and DLQ for error tracing.
    """
    def __init__(self) -> None:
        self._routers: Dict[str, Callable[..., Awaitable[Any]]] = {}
        self._pre_hooks: List[Callable[[Dict[str, Any]], Awaitable[None]]] = []
        self._post_hooks: List[Callable[[Dict[str, Any], Dict[str, Any]], Awaitable[None]]] = []
        self._lock = asyncio.Lock()

    async def initialize(self, module_map: Optional[Dict[str, str]] = None) -> None:
        """
        Discover and load plugins. If module_map is None, auto-discover under 'core.plugins'.
        """
        async with self._lock:
            mapping = module_map or self._auto_discover("core.plugins")
            for name, path in mapping.items():
                await self._load_plugin(name, path)

    def get_router(self, name: str) -> Optional[Callable[..., Awaitable[Any]]]:
        """Return the router hook for a given plugin name."""
        return self._routers.get(name)

    async def pre_trade(self, signal: Dict[str, Any]) -> None:
        """Execute all pre-trade hooks sequentially."""
        for fn in list(self._pre_hooks):
            try:
                with PLUG_LATENCY.labels(plugin=fn.__name__, stage="pre_trade").time():
                    await fn(signal)
            except Exception as exc:
                logger.error(f"[PluginManager] pre_trade {fn.__name__} failed: {exc}")
                PLUG_ERRORS.labels(plugin=fn.__name__).inc()
                FAILED_PLUGIN_DLQ.append({"hook": fn.__name__, "signal": signal, "error": str(exc)})

    async def post_trade(self, signal: Dict[str, Any], result: Dict[str, Any]) -> None:
        """Execute all post-trade hooks sequentially."""
        for fn in list(self._post_hooks):
            try:
                with PLUG_LATENCY.labels(plugin=fn.__name__, stage="post_trade").time():
                    await fn(signal, result)
            except Exception as exc:
                logger.error(f"[PluginManager] post_trade {fn.__name__} failed: {exc}")
                PLUG_ERRORS.labels(plugin=fn.__name__).inc()
                FAILED_PLUGIN_DLQ.append({"hook": fn.__name__, "signal": signal, "result": result, "error": str(exc)})

    async def hot_reload(self, name: str, import_path: str) -> None:
        """Reloads a specific plugin at runtime."""
        async with self._lock:
            await self._load_plugin(name, import_path, reload_=True)

    def _auto_discover(self, package: str) -> Dict[str, str]:
        """Auto-discover plugin modules in a package."""
        mapping: Dict[str, str] = {}
        pkg = importlib.import_module(package)
        for finder, mod_name, is_pkg in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
            if not is_pkg:
                key = mod_name.split('.')[-1]
                mapping[key] = mod_name
        return mapping

    async def _load_plugin(self, name: str, import_path: str, reload_: bool = False) -> None:
        """Load or reload a plugin module and register its hooks."""
        try:
            module = importlib.import_module(import_path)
            if reload_:
                module = importlib.reload(module)
            logger.info(f"[PluginManager] Loaded plugin {name} from {import_path}")
            PLUG_LOADS.labels(plugin=name).inc()

            # Router
            fn_router = getattr(module, "router", None)
            if callable(fn_router) and inspect.iscoroutinefunction(fn_router):
                self._routers[name] = fn_router

            # Pre-trade hooks
            fn_pre = getattr(module, "pre_trade", None)
            if callable(fn_pre) and inspect.iscoroutinefunction(fn_pre):
                self._pre_hooks.append(fn_pre)

            # Post-trade hooks
            fn_post = getattr(module, "post_trade", None)
            if callable(fn_post) and inspect.iscoroutinefunction(fn_post):
                self._post_hooks.append(fn_post)

            # Registration callback
            fn_reg = getattr(module, "register", None)
            if callable(fn_reg):
                await asyncio.to_thread(fn_reg, self)

        except Exception as exc:
            logger.error(f"[PluginManager] Failed to load plugin {name}: {exc}")
            PLUG_ERRORS.labels(plugin=name).inc()
            FAILED_PLUGIN_DLQ.append({"plugin": name, "path": import_path, "error": str(exc)})

# Explicit exports
__all__ = [
    "fire_plugin_event",
    "register_plugin_event_hook",
    "PluginManager",
    "FAILED_PLUGIN_DLQ",
]
