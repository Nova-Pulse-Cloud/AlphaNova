import hashlib
import logging
import uuid
from typing import Dict, Any
from prometheus_client import Counter, Histogram

from core.token_auditor import TokenAuditor
from core.reinforcement_learning import DQNAgent, RLTrader
from core.signal_explainer_ai import SignalExplainerAI
from core.market_conditions import MarketConditionsService
from core.trade_memory import TradeMemory, TradeRecord
from core.memory_mesh import MemoryMesh
from core.plugin_manager import PluginManager
from utils.dlq_manager import get_all_dlq
from utils.pnl_image_card import generate_pnl_card

logger = logging.getLogger("unified_trade_executor")

# === Prometheus Metrics ===
EXECUTIONS = Counter("trade_executions_total", "Total trades executed")
EXECUTION_LATENCY = Histogram("trade_execution_latency_seconds", "Execution latency")
EXECUTION_SKIPS = Counter("trade_skips_total", "Trades skipped due to risk or market filter")
EXECUTION_ERRORS = Counter("trade_execution_errors_total", "Trade execution failures")

class UnifiedTradeExecutor:
    def __init__(
        self,
        wallet,
        memory: TradeMemory,
        auditor: TokenAuditor,
        explainer: SignalExplainerAI,
        market_filter: MarketConditionsService,
        rl_agent: DQNAgent,
        rl_trader: RLTrader,
        mesh: MemoryMesh,
        plugin_manager: PluginManager
    ):
        self.wallet = wallet
        self.memory = memory
        self.auditor = auditor
        self.explainer = explainer
        self.market = market_filter
        self.agent = rl_agent
        self.rl = rl_trader
        self.mesh = mesh
        self.plugins = plugin_manager

    async def execute(self, symbol: str, meta: Dict[str, Any]) -> Dict[str, Any]:
        trace_id = str(uuid.uuid4())
        meta["trace_id"] = trace_id
        logger.info(f"[Executor:{trace_id}] Starting execution pipeline for {symbol}")
        
        with EXECUTION_LATENCY.time():
            try:
                # 1. Risk & Market Filter
                trust = await self.auditor.audit_token(symbol)
                if getattr(trust, "risk", 0.0) > 0.7:
                    logger.warning(f"[Executor:{trace_id}] Token {symbol} flagged as high risk.")
                    EXECUTION_SKIPS.inc()
                    return {"status": "skipped", "reason": "high risk", "trust": trust.dict(), "trace_id": trace_id}

                market_ok = await self.market.is_favorable(symbol)
                if not market_ok:
                    logger.warning(f"[Executor:{trace_id}] Market conditions unfavorable for {symbol}.")
                    EXECUTION_SKIPS.inc()
                    return {"status": "skipped", "reason": "market filter", "trace_id": trace_id}

                # 2. RL Decision (DQN/Hybrid)
                state_vector = self._build_state_vector(meta, trust)
                action = await self.agent.act(state_vector) if hasattr(self.agent, "act") and callable(self.agent.act) else self.agent.act(state_vector)
                if isinstance(action, dict) and "action" in action:
                    action = action["action"]  # Support RLTrader style

                if action == 0 or action == "hold":
                    logger.info(f"[Executor:{trace_id}] Action HOLD on {symbol}")
                    return {"status": "held", "score": meta, "trust": trust.dict(), "trace_id": trace_id}

                side = "buy" if action == 1 or action == "enter" else "sell"
                tx = await self._route_trade(symbol, side, trace_id=trace_id)
                EXECUTIONS.inc()
                logger.info(f"[Executor:{trace_id}] Executed {side.upper()} for {symbol}: {tx}")

                # 3. Explanation + PnL card
                rationale = await self.explainer.explain_signal(meta) if hasattr(self.explainer, "explain_signal") else await self.explainer.explain(symbol)
                pnl_card = await generate_pnl_card(symbol)

                # 4. Memory, RL reward, Mesh, and Plugin post-trade
                record = TradeRecord(
                    symbol=symbol,
                    strategy="alpha",
                    outcome=tx.get("pnl", 0.0),
                    entry=tx.get("price", 0.0),
                    risk=getattr(trust, "risk", 0.0),
                    flavor=side,
                    agents=meta,
                    timestamp=tx.get("timestamp", 0)
                )
                await self.memory.record_signal(record)
                await self.rl.reward(meta, {"pnl": tx.get("pnl", 0.0)}, user=meta.get("user_id"))
                await self.mesh.log(
                    symbol=symbol,
                    entry_price=tx.get("price", 0.0),
                    outcome=tx.get("pnl", 0.0),
                    strategy="alpha",
                    features=meta,
                    context=meta,
                    timestamp=tx.get("timestamp")
                )
                if hasattr(self.plugins, "post_trade"):
                    await self.plugins.post_trade(meta, tx)

                execution_hash = self._get_execution_hash(symbol, side, meta)

                return {
                    "status": "executed",
                    "symbol": symbol,
                    "side": side,
                    "tx": tx,
                    "trust": trust.dict() if hasattr(trust, "dict") else dict(trust),
                    "rationale": rationale,
                    "pnl_card": pnl_card,
                    "execution_hash": execution_hash,
                    "trace_id": trace_id,
                }

            except Exception as e:
                EXECUTION_ERRORS.inc()
                try:
                    from core.execution_router import FAILED_EXEC_DLQ
                except ImportError:
                    FAILED_EXEC_DLQ = []
                FAILED_EXEC_DLQ.append({"symbol": symbol, "meta": meta, "error": str(e)})
                logger.exception(f"[Executor:{trace_id}] Execution failed for {symbol}")
                return {"status": "error", "reason": str(e), "trace_id": trace_id}

    def _build_state_vector(self, meta: Dict, trust) -> list:
        """Build state vector for RL agent from signal meta and trust data."""
        volume_score = getattr(trust, "volume_score", 0.0)
        liquidity_score = getattr(trust, "liquidity_score", 0.0)
        return [
            meta.get("confidence", 0.0),
            meta.get("social_sentiment", 0.0),
            meta.get("volume", 0.0),
            getattr(trust, "risk", 0.0),
            volume_score,
            liquidity_score
        ]

    async def _route_trade(self, symbol: str, side: str, trace_id: str) -> Dict:
        """Route the trade through the plugin manager."""
        router = getattr(self.plugins, "get_router", None)
        if router and callable(router):
            route_fn = router("phantom")
            if route_fn:
                return await route_fn(symbol=symbol, side=side, wallet=self.wallet, trace_id=trace_id)
        # Fallback (legacy)
        route_trade = getattr(self.plugins, "route_trade", None)
        if route_trade and callable(route_trade):
            return await route_trade(symbol=symbol, side=side, wallet=self.wallet, trace_id=trace_id)
        raise RuntimeError("No trade router available for Phantom wallet")

    def _get_execution_hash(self, symbol: str, side: str, meta: Dict) -> str:
        """Generate a unique hash for this trade execution."""
        raw = f"{symbol}:{side}:{meta.get('trace_id', '')}:{meta}"
        return hashlib.sha256(raw.encode()).hexdigest()
