# File: core/execution/execution_router.py
# 🚀 Nova Pulse ALPHA – Execution Router (Elite Edition)
"""Central async orchestrator: RL‑optimized routing, risk checks, plugin hooks,
Prometheus metrics, OpenTelemetry tracing, DLQ & audit hashing. Worthy of #1 bot."""

import asyncio
import hashlib
import logging
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional

import httpx
from opentelemetry import trace
from prometheus_client import Counter, Histogram

from utils.config import Config
from core.neural_timeout_manager import NeuralTimeoutManager
from utils.plugin_manager import PluginManager
from core.signal_generation.reinforcement_learning import RLTrader
from core.signal_generation.sl_manager import get_best_tp_sl as calc_sl, slm_feedback
from utils.trade_logger import log_trade
from utils.token_auditor import TokenAuditor
from utils.token_guardian import TokenGuardian
from telegram.integration.telegram_router import push_trade_update
from core.data_collection.orderbook_overlay import get_orderbook_imbalance

logger = logging.getLogger("nova.execution_router")
logger.setLevel(logging.INFO)
tracer = trace.get_tracer(__name__)

# ────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────
RPC_CALLS   = Counter("execution_router_rpc_calls_total", "Total RPC calls made")
RPC_LATENCY = Histogram("execution_router_rpc_latency_seconds", "Latency of RPC calls")
RPC_ERRORS  = Counter("execution_router_rpc_errors_total", "RPC execution errors")
EXEC_DLQ    = Counter("execution_router_dlq_total", "Failed executions routed to DLQ")

# ────────────────────────────────
# DLQ & Event Hooks
# ────────────────────────────────
FAILED_EXEC_DLQ: List[Dict[str, Any]] = []
execution_router_event_hooks: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

# ────────────────────────────────
# Limits (override via Config if needed)
# ────────────────────────────────
MAX_TOTAL_SOL = 0.5
MAX_TRADE_SOL = 0.1

# ────────────────────────────────
# Helpers
# ────────────────────────────────
def register_execution_router_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    execution_router_event_hooks.append(hook_fn)

async def _fire_event(name: str, data: Dict[str, Any]):
    for hook in execution_router_event_hooks:
        try:
            await hook(name, data)
        except Exception as e:
            logger.error(f"[ExecutionRouter Event] Hook failure {hook.__name__}: {e}")

def _execution_hash(signal: Dict[str, Any], result: Dict[str, Any]) -> str:
    m = hashlib.sha256()
    m.update(str(signal).encode())
    m.update(str(result).encode())
    return m.hexdigest()

# ────────────────────────────────
# ExecutionRouter Class
# ────────────────────────────────
class ExecutionRouter:
    """Async, RL‑driven, plugin‑based execution engine."""

    def __init__(
        self,
        wallet_manager: Any,
        rpc_nodes: List[str],
        *,
        config: Optional[Config] = None,
    ):
        self.cfg            = config or Config()
        self.wallet_manager = wallet_manager
        self.rpc_nodes      = rpc_nodes
        self.plugin_mgr     = PluginManager()
        self.timeout_mgr    = NeuralTimeoutManager()
        self.http           = httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))
        self.sem            = asyncio.Semaphore(int(self.cfg.EXECUTION_RATE_LIMIT))
        self.rl_trader      = RLTrader()
        self.auditor        = TokenAuditor(redis_url=wallet_manager.redis_url)
        self.guardian       = TokenGuardian(helius_api_key=self.cfg.HELIUS_API_KEY)

    # ────────────────────────────────
    # Lifecycle
    # ────────────────────────────────
    async def connect(self):
        await self.plugin_mgr.initialize()
        await self.auditor.connect()
        await self.guardian.connect()
        logger.info("✅ ExecutionRouter ready.")

    async def disconnect(self):
        await self.http.aclose()
        await self.auditor.disconnect()
        await self.guardian.close()
        logger.info("🔕 ExecutionRouter shut down.")

    # ────────────────────────────────
    # Internal Helpers
    # ────────────────────────────────
    async def _choose_rpc(self, signal: Dict[str, Any]) -> str:
        RPC_CALLS.inc()
        idx = self.rl_trader.pick_best_node(self.rpc_nodes, signal)
        return self.rpc_nodes[idx]

    async def _risk_checks(self, signal: Dict[str, Any]) -> Optional[str]:
        total_sol = await self.wallet_manager.get_total_open_sol()
        if total_sol >= MAX_TOTAL_SOL:
            return "Max exposure reached"
        if signal.get("size", 0) > MAX_TRADE_SOL:
            return "Trade size exceeds limit"
        if await self.wallet_manager.has_open_position(signal["symbol"]):
            return "Position already open"
        audit = await self.auditor.audit_token(signal["symbol"])
        if audit.risk > self.cfg.MAX_RISK_THRESHOLD:
            return "Token audit risk too high"
        guardian = await self.guardian.audit(signal["symbol"])
        if not guardian.get("safe", False):
            return f"Guardian blocked: {guardian.get('reason')}"
        return None

    # ────────────────────────────────
    # Public: execute_trade
    # ────────────────────────────────
    async def execute_trade(self, signal: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
        async with self.sem, tracer.start_as_current_span("execute_trade"):
            try:
                signal.setdefault("trace_id", f"tx-{uuid.uuid4().hex[:8]}")

                # Confidence check
                if signal.get("score", 0) < self.cfg.MIN_CONFIDENCE:
                    return {"status": "skipped", "reason": "low confidence", "trace_id": signal["trace_id"]}

                # RL sizing
                signal["size"] = self.rl_trader.act(signal)

                # Risk checks
                if err := await self._risk_checks(signal):
                    return {"status": "rejected", "reason": err, "trace_id": signal["trace_id"]}

                # Orderbook adjustment
                ob = await get_orderbook_imbalance(signal["symbol"])
                signal["slippage_adjustment"] = ob.get("imbalance_score", 0.0)

                # Pre‑trade plugins
                await self.plugin_mgr.pre_trade(signal)

                # TP/SL Calculation
                tp, sl, sl_meta = await calc_sl(signal, user=user_id)
                signal.update({"tp": tp, "sl": sl})

                # Size adjust for slippage
                signal["size"] *= max(1 - signal.get("slippage_adjustment", 0), 0.5)

                # Choose RPC
                rpc = await self._choose_rpc(signal)

                # Dispatch via router plugin (assumes "phantom" router registered)
                router_fn = self.plugin_mgr.get_router("phantom")
                tx_result = await router_fn(signal=signal, rpc_url=rpc)

                # Feedback loops
                await slm_feedback(signal, tx_result)
                await self.rl_trader.reward(signal, tx_result)

                # Audit & log
                tx_result.update({
                    "audit_hash": _execution_hash(signal, tx_result),
                    "trace_id": signal["trace_id"],
                })
                await log_trade(
                    symbol=signal["symbol"],
                    side=signal["side"],
                    size=signal["size"],
                    entry=tx_result.get("entry_price"),
                    exit=tx_result.get("exit_price"),
                    pnl=tx_result.get("pnl"),
                    user=user_id,
                )

                # Notify
                await push_trade_update(signal, tx_result)
                await _fire_event("trade_success", {"signal": signal, "result": tx_result})
                return tx_result

            except Exception as e:
                RPC_ERRORS.inc()
                EXEC_DLQ.inc()
                fail = {"signal": signal, "error": str(e), "user_id": user_id}
                FAILED_EXEC_DLQ.append(fail)
                await _fire_event("trade_failed", fail)
                return {"status": "failed", "error": str(e), "trace_id": signal.get("trace_id")}
