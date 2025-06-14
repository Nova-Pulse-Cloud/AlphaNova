# File: core/execution/unified_trade_executor.py

import logging
import hashlib
import uuid
from typing import Dict, Any

from prometheus_client import Counter, Histogram

from utils.config import Config
from utils.token_auditor import TokenAuditor
from core.signal_generation.reinforcement_learning import DQNAgent, RLTrader
from core.signal_generation.signal_explainer_ai import SignalExplainerAI
from core.data_collection.market_conditions import MarketConditionsService
from utils.trade_memory import TradeMemory
from core.alpha_memory_mesh import MemoryMesh
from utils.plugin_manager import PluginManager
from utils.dlq_manager import get_all_dlq
from utils.pnl_card import generate_pnl_card

logger = logging.getLogger("unified_trade_executor")
logger.setLevel(logging.INFO)

# === Prometheus Metrics ===
EXECUTIONS       = Counter("trade_executions_total", "Total trades executed")
EXECUTION_LATENCY = Histogram("trade_execution_latency_seconds", "Execution latency")
EXECUTION_SKIPS   = Counter("trade_skips_total", "Trades skipped due to risk or market filter")
EXECUTION_ERRORS  = Counter("trade_execution_errors_total", "Trade execution failures")

# Dead-letter queue for failed executions
FAILED_EXEC_DLQ: list = []

class UnifiedTradeExecutor:
    def __init__(
        self,
        config: Config,
        wallet_manager,
        memory: TradeMemory,
        auditor: TokenAuditor,
        explainer: SignalExplainerAI,
        market_filter: MarketConditionsService,
        rl_agent: DQNAgent,
        rl_trader: RLTrader,
        mesh: MemoryMesh,
        plugin_manager: PluginManager
    ):
        self.config         = config
        self.wallet_manager = wallet_manager
        self.memory         = memory
        self.auditor        = auditor
        self.explainer      = explainer
        self.market         = market_filter
        self.agent          = rl_agent
        self.rl_trader      = rl_trader
        self.mesh           = mesh
        self.plugins        = plugin_manager

    async def execute_trade(
        self,
        signal: Dict[str, Any],
        user_id: Any = None
    ) -> Dict[str, Any]:
        trace_id = signal.get("trace_id") or str(uuid.uuid4())
        logger.info(f"[Executor:{trace_id}] Pipeline start: {signal}")

        with EXECUTION_LATENCY.time():
            try:
                symbol = signal["symbol"]
                side   = signal["side"]

                # 1️⃣ Risk & Market Filter
                trust = await self.auditor.audit_token(symbol)
                if trust.risk > self.config.MAX_RISK_THRESHOLD:
                    EXECUTION_SKIPS.inc()
                    return {
                        "status": "skipped",
                        "reason": "risk",
                        "trust": trust.to_dict(),
                        "trace_id": trace_id,
                    }

                if not await self.market.is_favorable(symbol):
                    EXECUTION_SKIPS.inc()
                    return {
                        "status": "skipped",
                        "reason": "market",
                        "trace_id": trace_id,
                    }

                # 2️⃣ RL Decision
                state_vector = self._build_state(signal, trust)
                action = await self.agent.act(state_vector)
                if action in (0, "hold"):
                    return {"status": "held", "trace_id": trace_id}

                # 3️⃣ Trade Execution
                tx = await self.plugins.route_trade(
                    symbol=symbol,
                    side=side,
                    wallet=self.wallet_manager.get_wallet("solana"),
                    trace_id=trace_id,
                )
                EXECUTIONS.inc()

                # 4️⃣ Explanation & PnL
                rationale = await self.explainer.explain_signal(signal)
                pnl_card  = await generate_pnl_card(tx)

                # 5️⃣ Record & Reward & Mesh & Plugins
                record = {
                    "symbol": symbol,
                    "side": side,
                    "pnl": tx.get("pnl", 0.0),
                    "price": tx.get("price"),
                    "trace_id": trace_id,
                }
                await self.memory.add_trade(symbol, record)
                await self.rl_trader.reward(signal, {"pnl": record["pnl"]}, user_id)
                await self.mesh.log_trade(record, signal)
                if hasattr(self.plugins, "post_trade"):
                    await self.plugins.post_trade(signal, tx)

                exec_hash = hashlib.sha256(f"{symbol}:{side}:{trace_id}".encode()).hexdigest()
                return {
                    "status": "executed",
                    "trace_id": trace_id,
                    "pnl_card": pnl_card,
                    "rationale": rationale,
                    "execution_hash": exec_hash,
                }

            except Exception as e:
                EXECUTION_ERRORS.inc()
                logger.exception(f"[Executor:{trace_id}] Error: {e}")
                FAILED_EXEC_DLQ.append({"signal": signal, "error": str(e)})
                return {"status": "error", "reason": str(e), "trace_id": trace_id}

    def _build_state(self, signal: Dict[str, Any], trust) -> list:
        return [
            signal.get("score", 0.0),
            trust.risk,
            signal.get("volatility", 0.0),
        ]
