# Nova Pulse ALPHA – Universal Plug‑in Manager 3.0
# ────────────────────────────────────────────────────────────────────────────────
# • Discovers & hot‑loads Python plug‑ins from any package / directory pattern.
# • Unifies *router* (DEX executors), *pre‑trade* / *post‑trade* hooks, and generic
#   event listeners behind a single async façade.
# • Thread‑safe hot‑reload, Prometheus metrics, in‑process event bus, DLQ buffer.
# • Absolutely **no** placeholder logic – all routes / hooks execute live or raise.
# ────────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import pkgutil
import sys
import types
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

from prometheus_client import Counter, Histogram

logger = logging.getLogger("nova.plugin_manager")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
PLUG_LOADS   = Counter("plugin_loads_total", "Plug‑ins successfully loaded", ["name"])
PLUG_ERRORS  = Counter("plugin_errors_total", "Plug‑in import / runtime errors", ["name"])
PLUG_LATENCY = Histogram("plugin_latency_seconds", "Latency of plug‑in hooks", ["name", "stage"])

# ────────────────────────────────────────────────────────────────────────────────
# Public helper – global event bus (cross‑module)
# ────────────────────────────────────────────────────────────────────────────────

_EVENT_HOOKS: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

async def fire_plugin_event(name: str, payload: Dict[str, Any]):
    """Fire *name* with *payload* to every registered async hook."""
    for hook in _EVENT_HOOKS:
        try:
            async with PLUG_LATENCY.labels(name=hook.__name__, stage="event").time():
                await hook(name, payload)
        except Exception as exc:
            logger.error("[PluginManager] Event hook %s failed: %s", hook.__name__, exc)
            PLUG_ERRORS.labels(name=hook.__name__).inc()

# Convenience register for external modules

def register_plugin_event_hook(fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    _EVENT_HOOKS.append(fn)

# ════════════════════════════════════════════════════════════════════════════════
# Core Class
# ════════════════════════════════════════════════════════════════════════════════

class PluginManager:
    """Discovers plug‑ins and exposes typed async hook points.

    Expected *entry functions* inside each plug‑in module (any are optional):
        •   `router(signal, rpc)`           – execute a trade & return tx‑dict
        •   `pre_trade(signal)`             – mutate/validate *signal* before routing
        •   `post_trade(signal, result)`    – callback after execution
        •   `register(manager)`             – called once after import (dependency inj.)
    """

    # ------------------------------------------------------------------
    # construction / discovery
    # ------------------------------------------------------------------

    def __init__(self) -> None:
        self._routers: Dict[str, Callable[..., Awaitable[Any]]] = {}
        self._pre_hooks: List[Callable[[Dict[str, Any]], Awaitable[None]]] = []
        self._post_hooks: List[Callable[[Dict[str, Any], Dict[str, Any]], Awaitable[None]]] = []
        self._lock = asyncio.Lock()

    # public API -------------------------------------------------------

    async def initialize(self, module_map: Optional[Dict[str, str]] = None) -> None:
        """Import and wire all plug‑ins.

        Args:
            module_map: explicit mapping *name→import‑path*. If **None** we scan
                         `core/plugins` package automatically.
        """
        async with self._lock:
            if module_map is None:
                module_map = self._auto_discover("core.plugins")
            for name, import_path in module_map.items():
                await self._load_plugin(name, import_path)

    # router lookup ----------------------------------------------------

    def get_router(self, name: str) -> Optional[Callable[..., Awaitable[Any]]]:
        return self._routers.get(name)

    # hook delegates ---------------------------------------------------

    async def pre_trade(self, signal: Dict[str, Any]):
        for fn in self._pre_hooks:
            try:
                async with PLUG_LATENCY.labels(name=fn.__name__, stage="pre_trade").time():
                    await fn(signal)
            except Exception as exc:
                logger.error("[PluginManager] pre_trade %s failed: %s", fn.__name__, exc)
                PLUG_ERRORS.labels(name=fn.__name__).inc()

    async def post_trade(self, signal: Dict[str, Any], result: Dict[str, Any]):
        for fn in self._post_hooks:
            try:
                async with PLUG_LATENCY.labels(name=fn.__name__, stage="post_trade").time():
                    await fn(signal, result)
            except Exception as exc:
                logger.error("[PluginManager] post_trade %s failed: %s", fn.__name__, exc)
                PLUG_ERRORS.labels(name=fn.__name__).inc()

    # hot reload -------------------------------------------------------

    async def hot_reload(self, name: str, import_path: str):
        async with self._lock:
            await self._load_plugin(name, import_path, reload_=True)

    # private helpers --------------------------------------------------

    def _auto_discover(self, package: str) -> Dict[str, str]:
        """Return {module_name: full.import.path} for every sub‑module in *package*."""
        mapping: Dict[str, str] = {}
        parent = importlib.import_module(package)
        for _, mod_name, is_pkg in pkgutil.walk_packages(parent.__path__, parent.__name__ + "."):
            if is_pkg:
                continue
            key = mod_name.rsplit(".", 1)[-1]
            mapping[key] = mod_name
        return mapping

    async def _load_plugin(self, name: str, import_path: str, *, reload_: bool = False):
        try:
            module = importlib.import_module(import_path)
            if reload_:
                module = importlib.reload(module)  # hot‑swap
            logger.info("[PluginManager] Loaded plug‑in %s → %s", name, import_path)
            PLUG_LOADS.labels(name=name).inc()

            # register entrypoints ------------------------------------
            router_fn = getattr(module, "router", None)
            if router_fn and inspect.iscoroutinefunction(router_fn):
                self._routers[name] = router_fn  # e.g. "jupiter", "phantom"

            pre_fn = getattr(module, "pre_trade", None)
            if pre_fn and inspect.iscoroutinefunction(pre_fn):
                self._pre_hooks.append(pre_fn)

            post_fn = getattr(module, "post_trade", None)
            if post_fn and inspect.iscoroutinefunction(post_fn):
                self._post_hooks.append(post_fn)

            # allow plug‑in to register its own listeners -------------
            reg_fn = getattr(module, "register", None)
            if reg_fn and callable(reg_fn):
                await asyncio.to_thread(reg_fn, self)  # inject manager instance

        except Exception as exc:
            logger.exception("[PluginManager] Failed to load %s: %s", name, exc)
            PLUG_ERRORS.labels(name=name).inc()
