import asyncio
import logging
import hashlib
import uuid
from typing import List, Dict, Any, Optional, Callable, Awaitable
import httpx
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential
from prometheus_client import Counter, Histogram
from opentelemetry import trace

from core.neural_timeout_manager import NeuralTimeoutManager
from core.plugin_manager import PluginManager
from core.reinforcement_learning import RLTrader
from core.sl_manager import SLManager
from core.ml_tp_sl import tpsl_rl_feedback
from utils.trade_logger import log_trade
from utils.token_auditor import TokenAuditor
from utils.token_guardian import TokenGuardian
from telegram.telegram_router import push_trade_update
from core.orderbook_overlay import get_orderbook_imbalance  # OrderBookOverlay import

# Set up OpenTelemetry tracing
logger = logging.getLogger("nova.execution_router")
tracer = trace.get_tracer(__name__)

# Prometheus Metrics
RPC_CALLS = Counter("execution_router_rpc_calls_total", "Total RPC calls made")
RPC_LATENCY = Histogram("execution_router_rpc_latency_seconds", "Latency of RPC calls")
RPC_ERRORS = Counter("execution_router_rpc_errors_total", "RPC execution errors")
EXEC_DLQ = Counter("execution_router_dlq_total", "Failed executions routed to DLQ")

FAILED_EXEC_DLQ = []
execution_router_event_hooks = []

MAX_TOTAL_SOL = 0.5
MAX_TRADE_SOL = 0.1

rl_trader = RLTrader()
sl_manager = SLManager()

def register_execution_router_hook(hook_fn: Callable[[str, Dict[str, Any]], Awaitable[None]]):
    execution_router_event_hooks.append(hook_fn)

async def fire_execution_router_event(event_name: str, data: Dict[str, Any]):
    for hook in execution_router_event_hooks:
        try:
            await hook(event_name, data)
        except Exception as e:
            logger.error(f"[Nova][ExecutionRouter Event] Hook failure [{hook.__name__}]: {e}")

def execution_hash(signal: dict, result: dict) -> str:
    m = hashlib.sha256()
    m.update(str(signal).encode())
    m.update(str(result).encode())
    return m.hexdigest()

class ExecutionRouter:
    """
    Nova ALPHA: ExecutionRouter for async, RL-optimized, multi-chain, plugin-based trading.
    This is the heart of trade execution, integrating RL, token auditing, risk management,
    and dynamic routing to the best venues.
    """
    def __init__(self, wallet_manager: Any, rpc_nodes: List[str], helius_api_key: str, rate_limit: float = 10.0):
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
        self.wallet_manager = wallet_manager
        self.rpc_nodes = rpc_nodes
        self.helius_api_key = helius_api_key
        self.timeout_manager = NeuralTimeoutManager()
        self.plugin_manager = PluginManager()
        self.client = httpx.AsyncClient(limits=limits)
        self.rate_limit = rate_limit
        self._lock = asyncio.Semaphore(int(rate_limit))
        self.auditor = TokenAuditor(redis_url=wallet_manager.redis_url)
        self.guardian = TokenGuardian(helius_api_key=helius_api_key)

    async def connect(self) -> None:
        """Initialize all plugin and audit subsystems."""
        await self.plugin_manager.initialize()
        await self.auditor.connect()
        await self.guardian.connect()
        logger.info("✅ [Nova][ExecutionRouter] Initialized.")

    async def disconnect(self) -> None:
        """Clean shutdown for all clients and plugins."""
        await self.client.aclose()
        await self.auditor.disconnect()
        await self.guardian.close()
        logger.info("🔕 [Nova][ExecutionRouter] Shut down.")

    async def _choose_rpc(self, signal: Dict[str, Any]) -> str:
        """Select optimal RPC node via RL agent."""
        idx = rl_trader.pick_best_node(self.rpc_nodes, signal)
        return self.rpc_nodes[idx]

    async def _enforce_limits(self, signal: Dict[str, Any]) -> Optional[str]:
        """Live risk limits, exposure checks, and token audits."""
        total_sol = await self.wallet_manager.get_total_open_sol()
        if total_sol >= MAX_TOTAL_SOL:
            return "Max exposure reached (0.5 SOL)"
        if signal.get("size", 0.0) > MAX_TRADE_SOL:
            return "Trade size too large (> 0.1 SOL)"
        symbol = signal.get("symbol")
        if await self.wallet_manager.has_open_position(symbol):
            return f"Already have open position for {symbol}"
        audit = await self.auditor.audit_token(signal.get("address"))
        if not audit.is_safe:
            return f"TokenAudit failed: {audit.reason}"
        guardian = await self.guardian.audit(signal.get("address"))
        if not guardian.get("safe", False):
            return f"TokenGuardian blocked: {guardian.get('reason', 'unknown')}"
        return None

    async def execute_trade(self, signal: Dict[str, Any], user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        The main async trade executor. Handles RL sizing, SL/TP, plugin hooks, DLQ, all eventing and metrics.
        """
        async with self._lock, tracer.start_as_current_span("execute_trade") as span:
            try:
                signal.setdefault("trace_id", f"tx-{uuid.uuid4().hex[:8]}")

                # Ensure the signal has a valid score (from dynamic signal processing)
                if not signal.get("score") or signal["score"] < 0.7:
                    return {"status": "skipped", "reason": "low confidence", "trace_id": signal["trace_id"]}

                # Determine trade size using RL trader and signal
                signal["size"] = rl_trader.act(signal)

                # Enforce risk management and audit checks
                rejection_reason = await self._enforce_limits(signal)
                if rejection_reason:
                    return {"status": "rejected", "reason": rejection_reason, "trace_id": signal["trace_id"]}

                # Fetch order book imbalance from OrderBookOverlay
                orderbook_imbalance = await get_orderbook_imbalance(signal["symbol"])
                if orderbook_imbalance:
                    signal["slippage_adjustment"] = orderbook_imbalance.get("imbalance_score", 0.0)

                # Pre-trade plugin checks
                await self.plugin_manager.pre_trade(signal)

                # Calculate stop-loss, take-profit, etc., using a plugin
                sltp_result = await sl_manager.calc_sl(signal, plugin="ensemble", user=user_id)
                signal.update({"tp": sltp_result.get("tp"), "sl": sltp_result.get("sl"), "sltp_meta": sltp_result})

                # Adjust trade size and slippage based on order book
                signal["size"] *= max(1 - signal.get("slippage_adjustment", 0), 0.5)  # Example: reduce size by slippage score

                # Choose an RPC node dynamically
                rpc = await self._choose_rpc(signal)

                # Dispatch trade to the appropriate venue/router
                tx_result = await self._dispatch_trade(signal, rpc, span, user_id)

                # Provide RL feedback based on the results of the trade (PnL, success)
                await tpsl_rl_feedback(signal, sltp_result, tx_result)
                await self.plugin_manager.post_trade(signal, tx_result)

                # Audit trade results and record for future reference
                tx_result["audit_hash"] = execution_hash(signal, tx_result)
                tx_result["trace_id"] = signal["trace_id"]

                # Reward the RL trader based on performance
                await rl_trader.reward(signal, tx_result)
                await log_trade(
                    signal["symbol"], signal["side"], signal["size"], signal.get("entry", 0),
                    tx_result.get("exit_price", 0), tx_result.get("pnl", 0),
                    rationale=signal.get("rationale", "auto-signal"),
                    user=user_id or "global"
                )

                # Send trade update (optional integration with Telegram or other systems)
                await push_trade_update(signal, tx_result)
                await fire_execution_router_event("trade_success", {"signal": signal, "result": tx_result, "user_id": user_id})
                return tx_result

            except Exception as e:
                RPC_ERRORS.inc()
                EXEC_DLQ.inc()
                failed = {"signal": signal, "error": str(e), "user_id": user_id}
                FAILED_EXEC_DLQ.append(failed)
                await fire_execution_router_event("trade_failed", failed)
                return {"status": "failed", "error": str(e), "trace_id": signal.get("trace_id")}
