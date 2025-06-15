# core/execution/unified_trade_executor.py
"""
-------------------------------------------------------------------------------
World‑class unified execution pipeline:
• Multi‑layer risk filters (Audit + MarketConditions)
• RL decision (DQN or RLTrader)
• Plugin‑based routing + post‑hooks
• Prometheus metrics, DLQ, full audit hash
• LLM rationale + PnL card attachment
-------------------------------------------------------------------------------
"""
import asyncio
import hashlib
import logging
import uuid
from typing import Any, Dict, Optional

from prometheus_client import Counter, Histogram

from core.utils.config import Config
from core.utils.token_auditor import TokenAuditor
from core.signal_generation.reinforcement_learning import DQNAgent, RLTrader
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.data_collection.market_conditions import MarketConditionsService
from core.utils.trade_memory import TradeMemory
from core.alpha_memory_mesh import MemoryMesh
from core.utils.plugin_manager import PluginManager
from core.utils.pnl_card import generate_pnl_card
from utils.plugin_manager import fire_plugin_event

logger = logging.getLogger("nova.unified_trade_executor")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────
EXECUTIONS        = Counter("trade_executions_total", "Total trades executed")
EXECUTION_LATENCY = Histogram("trade_execution_latency_seconds", "Execution latency")
EXECUTION_SKIPS   = Counter("trade_skips_total", "Trades skipped")
EXECUTION_ERRORS  = Counter("trade_execution_errors_total", "Trade execution failures")

FAILED_EXEC_DLQ: list[Dict[str, Any]] = []

class UnifiedTradeExecutor:
    """High‑level façade that wires every subsystem into a single `execute()` call."""

    def __init__(
        self,
        *,
        config: Config,
        wallet_manager: Any,
        memory: TradeMemory,
        auditor: TokenAuditor,
        explainer: SignalExplainerAI,
        market_filter: MarketConditionsService,
        rl_agent: DQNAgent,
        rl_trader: RLTrader,
        mesh: MemoryMesh,
        plugin_manager: PluginManager,
    ) -> None:
        self.cfg           = config
        self.wallet_mgr    = wallet_manager
        self.memory        = memory
        self.auditor       = auditor
        self.explainer     = explainer
        self.market        = market_filter
        self.dqn           = rl_agent
        self.rl_trader     = rl_trader
        self.mesh          = mesh
        self.plugins       = plugin_manager

    async def execute_trade(self, signal: Dict[str, Any], *, user_id: Optional[str] = None) -> Dict[str, Any]:
        """End‑to‑end execution for **one** trading signal."""
        trace_id = signal.get("trace_id") or str(uuid.uuid4())
        logger.info(f"[UTE:{trace_id}] Start — {signal}")
        with EXECUTION_LATENCY.time():
            try:
                # ----------------------------------------------------------------
                # ① Risk / Market Filters
                # ----------------------------------------------------------------
                trust = await self.auditor.audit_token(signal["symbol"])
                if trust.risk > self.cfg.MAX_RISK_THRESHOLD:
                    EXECUTION_SKIPS.inc()
                    await fire_plugin_event("trade_skipped_risk", {"signal": signal, "trace_id": trace_id, "risk": trust.risk})
                    return {"status": "skipped", "reason": "risk", "trace_id": trace_id}
                if not await self.market.is_favorable(signal["symbol"]):
                    EXECUTION_SKIPS.inc()
                    await fire_plugin_event("trade_skipped_market", {"signal": signal, "trace_id": trace_id})
                    return {"status": "skipped", "reason": "market", "trace_id": trace_id}
                # ----------------------------------------------------------------
                # ② RL Action
                # ----------------------------------------------------------------
                state_vec = self._build_state(signal, trust)
                action = await self.dqn.act(state_vec)
                if action in (0, "hold"):
                    await fire_plugin_event("trade_held_rl", {"signal": signal, "trace_id": trace_id})
                    return {"status": "held", "trace_id": trace_id}
                side = "buy" if action in (1, "enter") else "sell"
                # ----------------------------------------------------------------
                # ③ Routing & Execution
                # ----------------------------------------------------------------
                try:
                    tx = await self.plugins.get_router(signal.get("dex", "phantom"))(
                        signal, self.cfg.HELIUS_RPC
                    )
                except Exception as router_exc:
                    EXECUTION_ERRORS.inc()
                    await fire_plugin_event("trade_router_error", {"signal": signal, "trace_id": trace_id, "error": str(router_exc)})
                    raise router_exc
                EXECUTIONS.inc()
                # ----------------------------------------------------------------
                # ④ Post‑processing (LLM explanation + PnL card)
                # ----------------------------------------------------------------
                rationale = await self.explainer.explain(signal)
                pnl_card  = await generate_pnl_card(signal["symbol"])
                # ----------------------------------------------------------------
                # ⑤ Persistence / Rewards / Mesh / Plugins
                # ----------------------------------------------------------------
                record = {
                    "symbol": signal["symbol"],
                    "side"  : side,
                    "pnl"   : tx.get("pnl", 0.0),
                    "price" : tx.get("price"),
                    "trace_id": trace_id,
                }
                await self.memory.add_trade(signal["symbol"], record)
                await self.rl_trader.reward(signal, {"pnl": record["pnl"]}, user_id)
                await self.mesh.log_trade(record, signal)
                if hasattr(self.plugins, "post_trade"):
                    await self.plugins.post_trade(signal, tx)
                exec_hash = hashlib.sha256(f"{signal['symbol']}:{side}:{trace_id}".encode()).hexdigest()
                await fire_plugin_event("trade_executed", {"signal": signal, "result": tx, "trace_id": trace_id, "side": side})
                return {
                    "status"   : "executed",
                    "trace_id" : trace_id,
                    "pnl_card" : pnl_card,
                    "rationale": rationale,
                    "execution_hash": exec_hash,
                }
            except Exception as e:
                EXECUTION_ERRORS.inc()
                logger.exception(f"[UTE:{trace_id}] Fatal: {e}")
                FAILED_EXEC_DLQ.append({"signal": signal, "error": str(e)})
                await fire_plugin_event("trade_failed", {"signal": signal, "error": str(e), "trace_id": trace_id})
                return {"status": "error", "reason": str(e), "trace_id": trace_id}

    @staticmethod
    def _build_state(signal: Dict[str, Any], trust) -> list:
        """State vector for DQN agent."""
        return [
            signal.get("score", 0.0),
            trust.risk,
            signal.get("volatility", 0.0),
        ]

# ────────────────────────────────────────────────────────────
# Factory Function (Example)
# ────────────────────────────────────────────────────────────
# To use in main_service or as a dependency:
def create_unified_trade_executor(cfg: Config) -> UnifiedTradeExecutor:
    return UnifiedTradeExecutor(
        config=cfg,
        wallet_manager=...,  # inject your WalletManager
        memory=...,          # inject TradeMemory
        auditor=...,         # inject TokenAuditor
        explainer=...,       # inject SignalExplainerAI
        market_filter=...,   # inject MarketConditionsService
        rl_agent=...,        # inject DQNAgent
        rl_trader=...,       # inject RLTrader
        mesh=...,            # inject MemoryMesh
        plugin_manager=...,  # inject PluginManager
    )
