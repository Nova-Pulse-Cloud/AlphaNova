# core/execution/execution_router.py

"""
Nova Pulse ALPHA – ExecutionRouter (Elite)
───────────────────────────────────────────────────────────────────────────────
- RL-optimized async routing
- Risk/cap enforcement (.1/.5 SOL, no dupes)
- Plugin-based wallet execution (Phantom-ready)
- Prometheus+Otel metrics
- Audit hash & DLQ
"""
import asyncio
import hashlib
import logging
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx
from opentelemetry import trace
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.utils.plugin_manager import PluginManager, fire_plugin_event
from core.wallet_management.wallet_manager import WalletManager
from core.signal_generation.reinforcement_learning import RLTrader
from core.utils.tpsl_jupiter_plugin import JupiterTPSLPlugin
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from core.utils.trade_logger import log_trade
from core.utils.token_utils import TokenUtils
from core.utils.trusted_authority_manager import audit_token

logger = logging.getLogger("nova.execution_router")
logger.setLevel(logging.INFO)
tracer = trace.get_tracer(__name__)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ────────────────────────────────────────────────────────────────────────────────
RPC_CALLS    = Counter("execution_router_rpc_calls_total", "Total RPC calls made")
RPC_LATENCY  = Histogram("execution_router_rpc_latency_seconds", "Latency of RPC calls")
RPC_ERRORS   = Counter("execution_router_rpc_errors_total", "RPC execution errors")
EXEC_DLQ     = Counter("execution_router_dlq_total", "Failed executions routed to DLQ")

# ────────────────────────────────────────────────────────────────────────────────
# Dead-letter queue & hooks
# ────────────────────────────────────────────────────────────────────────────────
FAILED_EXEC_DLQ: List[Dict[str, Any]] = []
execution_router_event_hooks: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

# ────────────────────────────────────────────────────────────────────────────────
# Limits (configurable)
# ────────────────────────────────────────────────────────────────────────────────
MAX_TOTAL_SOL = float(getattr(config, "MAX_TOTAL_SOL", 0.5))
MAX_TRADE_SOL = float(getattr(config, "MAX_TRADE_SOL", 0.1))

# ────────────────────────────────────────────────────────────────────────────────
# Helper to hash for audit
# ────────────────────────────────────────────────────────────────────────────────
def _execution_hash(signal: Dict[str, Any], result: Dict[str, Any]) -> str:
    m = hashlib.sha256()
    m.update(repr(signal).encode())
    m.update(repr(result).encode())
    return m.hexdigest()

class ExecutionRouter:
    """
    Central async orchestrator: RL‑optimized routing, risk checks, plugin hooks,
    Prometheus metrics, OpenTelemetry tracing, DLQ & audit hashing.
    """
    def __init__(
        self,
        wallet_manager: WalletManager,
        rpc_nodes: List[str],
    ) -> None:
        self.cfg            = config
        self.wallet_manager = wallet_manager
        self.rpc_nodes      = rpc_nodes
        self.plugin_mgr     = PluginManager()
        self.rl_trader      = RLTrader()
        self.tp_sl_plugin   = JupiterTPSLPlugin()
        self.token_utils    = TokenUtils()
        self._http          = httpx.AsyncClient(
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20)
        )
        self._sem           = asyncio.Semaphore(int(getattr(config, "EXECUTION_RATE_LIMIT", 10)))

    async def connect(self) -> None:
        await self.plugin_mgr.initialize()
        await self.token_utils.connect()
        logger.info("[ExecutionRouter] Connected and ready")

    async def disconnect(self) -> None:
        await self._http.aclose()
        await self.token_utils.disconnect()
        logger.info("[ExecutionRouter] Disconnected")

    async def _choose_rpc(self, signal: Dict[str, Any]) -> str:
        RPC_CALLS.inc()
        idx = self.rl_trader.pick_best_node(self.rpc_nodes, signal)
        return self.rpc_nodes[idx]

    async def _risk_checks(self, signal: Dict[str, Any]) -> Optional[str]:
        # Exposure limit
        total = await self.wallet_manager.get_total_open_sol()
        if total >= MAX_TOTAL_SOL:
            return "Max total exposure reached"
        # Single trade limit
        if signal.get("size", 0) > MAX_TRADE_SOL:
            return "Trade size exceeds limit"
        # Duplicate position
        if await self.wallet_manager.has_open_position(signal["symbol"]):
            return "Existing open position"
        # Token audit
        audit = await audit_token(signal["symbol"])
        if not audit.get("safe", False):
            return f"Token audit failed: {audit.get('reason')}"
        return None

    async def execute_trade(self, signal: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Main entry: process a trade signal end-to-end.
        Returns result dict with status, tx details, or error.
        """
        async with self._sem, tracer.start_as_current_span("execute_trade"):
            try:
                signal.setdefault("trace_id", f"tx-{uuid.uuid4().hex[:8]}")
                # Confidence & size
                if signal.get("score", 0) < float(getattr(config, "MIN_CONFIDENCE", 0.5)):
                    return {"status":"skipped","reason":"low confidence","trace_id": signal["trace_id"]}
                signal["size"] = self.rl_trader.act(signal)
                # Risk
                if err := await self._risk_checks(signal):
                    return {"status":"rejected","reason":err,"trace_id": signal["trace_id"]}
                # Slippage overlay
                imbalance, _ = await get_orderbook_imbalance(signal["symbol"])
                signal["slippage"] = imbalance
                # Pre-trade plugin hooks
                await self.plugin_mgr.pre_trade(signal)
                # TP/SL
                tp, sl, sl_meta = await self.tp_sl_plugin.compute(
                    symbol=signal["symbol"],
                    entry_price=signal.get("entry_price", 0),
                    alpha_signal=signal,
                    mode=signal.get("mode","live"),
                    user=user_id,
                )
                signal.update({"tp":tp, "sl":sl})
                # Choose RPC
                rpc = await self._choose_rpc(signal)
                # Route execution via plugin (e.g. phantom)
                router = self.plugin_mgr.get_router("phantom")
                result = await router(signal=signal, rpc_url=rpc)
                # Post-trade feedback
                await self.tp_sl_plugin.compute  # ensure any side-effects
                # RL reward
                await self.rl_trader.reward(signal, result)
                # Trade logging
                result.update({
                    "audit_hash": _execution_hash(signal, result),
                    "trace_id": signal["trace_id"],
                })
                await log_trade(
                    symbol=signal["symbol"],
                    side=signal["side"],
                    size=signal["size"],
                    entry=result.get("entry_price"),
                    exit=result.get("exit_price"),
                    pnl=result.get("pnl"),
                    user=user_id,
                )
                # Notify and fire events
                await fire_plugin_event("trade_success", {"signal":signal,"result":result})
                return result
            except Exception as e:
                RPC_ERRORS.inc()
                EXEC_DLQ.inc()
                failed = {"signal":signal,"error":str(e),"user_id":user_id}
                FAILED_EXEC_DLQ.append(failed)
                await fire_plugin_event("trade_failed", failed)
                return {"status":"failed","error":str(e),"trace_id": signal.get("trace_id")}

# alias for hooks
register_execution_router_hook = fire_plugin_event
