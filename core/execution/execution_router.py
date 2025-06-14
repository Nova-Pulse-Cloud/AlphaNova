# File: core/execution/execution_router.py

import asyncio
import logging
import hashlib
import uuid
from typing import List, Dict, Any, Optional, Callable, Awaitable

import httpx
from prometheus_client import Counter, Histogram
from opentelemetry import trace

from utils.config import Config
from core.neural_timeout_manager import NeuralTimeoutManager
from utils.plugin_manager import PluginManager
from core.signal_generation.reinforcement_learning import RLTrader
from core.signal_generation.sl_manager import SLManager
from ml.ml_tp_sl import tpsl_rl_feedback
from utils.trade_logger import log_trade
from utils.token_auditor import TokenAuditor
from utils.token_guardian import TokenGuardian
from telegram.integration.telegram_router import push_trade_update
from core.data_collection.orderbook_overlay import get_orderbook_imbalance

# Set up tracing and logging
logger = logging.getLogger("nova.execution_router")
logger.setLevel(logging.INFO)
tracer = trace.get_tracer(__name__)

# Prometheus Metrics
RPC_CALLS     = Counter("execution_router_rpc_calls_total", "Total RPC calls made")
RPC_LATENCY   = Histogram("execution_router_rpc_latency_seconds", "Latency of RPC calls")
RPC_ERRORS    = Counter("execution_router_rpc_errors_total", "RPC execution errors")
EXEC_DLQ      = Counter("execution_router_dlq_total", "Failed executions routed to DLQ")

# Dead-letter queue and event hooks
FAILED_EXEC_DLQ: List[Dict[str, Any]] = []
execution_router_event_hooks: List[Callable[[str, Dict[str, Any]], Awaitable[None]]] = []

# Trade limits (configurable via Config if desired)
MAX_TOTAL_SOL  = 0.5
MAX_TRADE_SOL  = 0.1


def register_execution_router_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    execution_router_event_hooks.append(hook_fn)


async def fire_execution_router_event(event_name: str, data: Dict[str, Any]):
    for hook in execution_router_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[ExecutionRouter Event] Hook failure {hook.__name__}: {e}")


def execution_hash(signal: Dict[str, Any], result: Dict[str, Any]) -> str:
    m = hashlib.sha256()
    m.update(str(signal).encode())
    m.update(str(result).encode())
    return m.hexdigest()


class ExecutionRouter:
    """
    Nova ALPHA: ExecutionRouter for async, RL-optimized, multi-chain, plugin-based trading.
    """
    def __init__(
        self,
        wallet_manager: Any,
        rpc_nodes: List[str],
        config: Config
    ):
        self.config           = config
        self.wallet_manager   = wallet_manager
        self.rpc_nodes        = rpc_nodes
        self.helius_api_key   = config.HELIUS_API_KEY
        self.timeout_manager  = NeuralTimeoutManager()
        self.plugin_manager   = PluginManager()
        self.client           = httpx.AsyncClient(limits=httpx.Limits(max_connections=100, max_keepalive_connections=20))
        self._lock            = asyncio.Semaphore(int(config.EXECUTION_RATE_LIMIT))
        self.auditor          = TokenAuditor(redis_url=wallet_manager.redis_url)
        self.guardian         = TokenGuardian(helius_api_key=self.helius_api_key)
        self.rl_trader        = RLTrader()
        self.sl_manager       = SLManager()

    async def connect(self) -> None:
        """Initialize plugin and audit subsystems."""
        await self.plugin_manager.initialize()
        await self.auditor.connect()
        await self.guardian.connect()
        logger.info("✅ ExecutionRouter initialized.")

    async def disconnect(self) -> None:
        """Clean shutdown."""
        await self.client.aclose()
        await self.auditor.disconnect()
        await self.guardian.close()
        logger.info("🔕 ExecutionRouter shut down.")

    async def _choose_rpc(self, signal: Dict[str, Any]) -> str:
        RPC_CALLS.inc()
        # RL-based node selection
        idx = self.rl_trader.pick_best_node(self.rpc_nodes, signal)
        return self.rpc_nodes[idx]

    async def _enforce_limits(self, signal: Dict[str, Any]) -> Optional[str]:
        total_sol = await self.wallet_manager.get_total_open_sol()
        if total_sol >= MAX_TOTAL_SOL:
            return f"Max exposure reached: {MAX_TOTAL_SOL} SOL"
        if signal.get("size", 0) > MAX_TRADE_SOL:
            return f"Trade size too large (> {MAX_TRADE_SOL} SOL)"
        if await self.wallet_manager.has_open_position(signal["symbol"]):
            return f"Position already open for {signal['symbol']}"
        audit = await self.auditor.audit_token(signal["symbol"])
        if audit.risk > config.MAX_RISK_THRESHOLD:
            return f"Audit flagged risk: {audit.risk}"
        guardian = await self.guardian.audit(signal["symbol"])
        if not guardian.get("safe", False):
            return f"Guardian blocked: {guardian.get('reason')}"
        return None

    async def execute_trade(
        self,
        signal: Dict[str, Any],
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Main trade execution pipeline."""
        async with self._lock, tracer.start_as_current_span("execute_trade"):
            try:
                signal.setdefault("trace_id", f"tx-{uuid.uuid4().hex[:8]}")

                # Confidence filter
                if signal.get("score", 0) < self.config.MIN_CONFIDENCE:
                    return {"status": "skipped", "reason": "low confidence", "trace_id": signal["trace_id"]}

                # Size decision
                signal["size"] = self.rl_trader.act(signal)

                # Risk checks
                rejection = await self._enforce_limits(signal)
                if rejection:
                    return {"status": "rejected", "reason": rejection, "trace_id": signal["trace_id"]}

                # Orderbook imbalance
                imbalance = await get_orderbook_imbalance(signal["symbol"])
                signal["slippage_adjustment"] = imbalance.get("imbalance_score", 0.0)

                # Pre-trade plugins
                await self.plugin_manager.pre_trade(signal)

                # Compute TP/SL
                sltp = await self.sl_manager.calc_sl(signal, plugin="ensemble", user=user_id)
                signal.update({"tp": sltp["tp"], "sl": sltp["sl"]})

                # Adjust size for slippage
                signal["size"] *= max(1 - signal.get("slippage_adjustment", 0), 0.5)

                # Choose RPC
                rpc = await self._choose_rpc(signal)

                # Dispatch
                tx_result = await self.plugin_manager.get_router("phantom")(signal, rpc)

                # RL feedback
                await tpsl_rl_feedback(signal, sltp, tx_result)
                await self.plugin_manager.post_trade(signal, tx_result)

                # Audit & log
                tx_result["audit_hash"] = execution_hash(signal, tx_result)
                tx_result["trace_id"]   = signal["trace_id"]
                await self.rl_trader.reward(signal, tx_result)
                await log_trade(
                    symbol=signal["symbol"], side=signal["side"], size=signal["size"],
                    entry=tx_result.get("entry_price"), exit=tx_result.get("exit_price"),
                    pnl=tx_result.get("pnl"), user=user_id
                )

                # Notifications
                await push_trade_update(signal, tx_result)
                await fire_execution_router_event("trade_success", {"signal": signal, "result": tx_result})

                return tx_result

            except Exception as e:
                RPC_ERRORS.inc()
                EXEC_DLQ.inc()
                error_payload = {"signal": signal, "error": str(e), "user_id": user_id}
                FAILED_EXEC_DLQ.append(error_payload)
                await fire_execution_router_event("trade_failed", error_payload)
                return {"status": "failed", "error": str(e), "trace_id": signal.get("trace_id")}
