# core/signal_generation/alpha_signal_agent.py
# 🚀 Nova Pulse ALPHA – AlphaSignalAgent v3.0: Dynamic, Fully Audited, TP/SL-Fused Signal Engine

import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import openai
import redis.asyncio as redis_client
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge

from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.data_collection.orderbook_overlay import OrderBookOverlay
from core.utils.plugin_manager import fire_plugin_event
from core.utils.risk_signals import TokenAuditor, TokenGuardian, rug_pull_signal
from core.agents.adaptive_tp_sl import AdaptiveTPSL  # Dynamic TP/SL agent

logger = logging.getLogger("nova.alpha_signal_agent")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus Metrics
# ────────────────────────────────────────────────────────────────────────────────
SIGNAL_TRIGGERS = Counter(
    "alpha_signal_triggers_total", "Total alpha signals triggered"
)
SIGNAL_ERRORS = Counter(
    "alpha_signal_errors_total", "Total alpha signal errors"
)
SIGNAL_LATENCY = Histogram(
    "alpha_signal_latency_seconds", "Alpha signal latency in seconds"
)
SIGNAL_SCORE = Gauge(
    "alpha_signal_score", "Last generated alpha signal score", ["symbol"]
)

# ────────────────────────────────────────────────────────────────────────────────
# Pydantic Model
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignal(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    trigger_sources: Dict[str, float]
    meta_features: Dict[str, Any]
    flavor: str
    score: float
    risk: float
    entry: float
    caption: str
    provenance_hash: str
    timestamp: int
    take_profit: float
    stop_loss: float
    audit_passed: bool = True
    audit_details: Optional[Dict[str, Any]] = None

# ────────────────────────────────────────────────────────────────────────────────
# Agent
# ────────────────────────────────────────────────────────────────────────────────
class AlphaSignalAgent:
    """
    AlphaSignalAgent fuses multi-agent signal inputs, overlays RL + trade memory,
    performs a full risk audit, and generates a production-grade, fully auditable signal object
    including unique dynamic TP/SL for each trade.
    """

    def __init__(
        self,
        redis_url: str = config.REDIS_URL,
        openai_key: str = config.OPENAI_API_KEY,
        symbols: Optional[List[str]] = None,
    ):
        self.redis_url = redis_url
        openai.api_key = openai_key
        self.redis: Optional[redis_client.Redis] = None
        self.trade_memory = TradeMemory(config)
        self.rl_tuner = ReinforcementTuner()
        self.orderbook = OrderBookOverlay(config)
        self.symbols = symbols or []
        self._caption_lock = asyncio.Lock()

        # Risk/audit agents
        self.token_auditor = TokenAuditor(config)
        self.token_guardian = TokenGuardian(config)
        # Adaptive TP/SL
        self.tpsl_agent = AdaptiveTPSL(config)

    async def connect(self):
        """Initialize Redis (DLQ storage, caching)."""
        if not self.redis:
            try:
                self.redis = await redis_client.from_url(self.redis_url, decode_responses=True)
                await self.redis.ping()
                logger.info("[AlphaSignalAgent] Redis connected")
            except Exception as e:
                logger.error(f"[AlphaSignalAgent] Redis connect failed: {e}")
                self.redis = None

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            logger.info("[AlphaSignalAgent] Redis disconnected")

    async def _generate_caption(self, symbol: str, meta: Dict[str, Any]) -> str:
        prompt = f"Write a viral, concise trading signal caption for {symbol} with context {meta}."
        async with self._caption_lock:
            for attempt in range(2):
                try:
                    resp = await openai.ChatCompletion.acreate(
                        model="gpt-3.5-turbo",
                        messages=[{"role": "user", "content": prompt}],
                        max_tokens=32,
                        temperature=0.7,
                    )
                    return resp.choices[0].message.content.strip()
                except Exception as e:
                    logger.warning(f"[AlphaSignalAgent] Caption attempt {attempt} failed: {e}")
                    await asyncio.sleep(2 ** attempt)
            return f"{symbol} signal generated! 🚀"

    def _compute_provenance(
        self, symbol: str, scores: Dict[str, float], meta: Dict[str, Any], flavor: str, score: float, risk: float, entry: float, tp: float, sl: float
    ) -> str:
        blob = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}{tp}{sl}"
        return hashlib.sha256(blob.encode()).hexdigest()

    def _choose_flavor(self, avg_score: float, user_context: Optional[Dict]) -> str:
        if user_context:
            risk_pref = user_context.get("risk")
            if risk_pref == "degen":
                return "Degen"
            if risk_pref == "safe":
                return "Safe"
        if avg_score > 0.85:
            return "Safe"
        if avg_score < 0.3:
            return "Degen"
        return "Moderate"

    async def _full_risk_audit(self, symbol: str) -> (bool, Dict[str, Any]):
        """
        Runs all audit layers: token audit, guardian, rug, whale, and returns (pass, details).
        """
        details = {}

        # TokenAuditor
        try:
            audit_result = await self.token_auditor.audit(symbol)
            details["TokenAuditor"] = audit_result
            if not audit_result.get("passed", False):
                return False, details
        except Exception as e:
            details["TokenAuditor"] = {"error": str(e)}
            return False, details

        # TokenGuardian
        try:
            guardian_result = await self.token_guardian.check(symbol)
            details["TokenGuardian"] = guardian_result
            if not guardian_result.get("passed", False):
                return False, details
        except Exception as e:
            details["TokenGuardian"] = {"error": str(e)}
            return False, details

        # Rug signal
        try:
            rug_result = await rug_pull_signal(symbol)
            details["RugSignal"] = rug_result
            if rug_result.get("is_rug", False):
                return False, details
        except Exception as e:
            details["RugSignal"] = {"error": str(e)}
            return False, details

        return True, details

    async def evaluate(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_context: Optional[Dict[str, Any]] = None,
    ) -> AlphaSignal:
        """
        Fuse multi-agent scores into one alpha signal with:
        - RL trust weighting
        - Trade-memory weighting
        - Market-depth adjustment
        - LLM captioning
        - Full risk/audit block
        - Dynamic TP/SL via AdaptiveTPSL
        - Provenance hashing
        - Prometheus metrics
        """
        SIGNAL_TRIGGERS.inc()
        start = datetime.utcnow()

        try:
            # ─────────────── Risk Audit (BLOCK if failed) ───────────────
            audit_passed, audit_details = await self._full_risk_audit(symbol)
            if not audit_passed:
                logger.warning(f"[AlphaSignalAgent] BLOCKED by audit: {symbol} | {audit_details}")
                raise RuntimeError(f"Signal blocked by audit for {symbol}: {audit_details}")

            # 1) Trust + memory weights
            weighted_scores = {}
            trust_map = {}
            memory_rate = await asyncio.get_event_loop().run_in_executor(
                None, self.trade_memory.get_win_rate, symbol
            )
            for sig in agent_signals:
                src = sig["source"]
                base = float(sig.get("score", 0.0))
                trust = self.rl_tuner.get_trust_score(src, symbol)
                weight = (trust + memory_rate) / 2.0
                weighted_scores[src] = base * weight
                trust_map[src] = weight

            # 2) Market-depth overlays
            ob = await self.orderbook.get_orderbook_data(symbol)
            liquidity = ob.get("liquidity", 0.0)
            whale_pressure = ob.get("whale_pressure", 0.0)
            depth_adj = 0.9 if liquidity < 0.1 else 1.0
            if whale_pressure > 0.8:
                depth_adj *= 1.1

            # 3) Composite score & risk
            avg = sum(weighted_scores.values()) / max(len(weighted_scores), 1)
            score = avg * depth_adj
            risk = 1.0 - min(trust_map.values()) if trust_map else 1.0
            entry = float(agent_signals[0].get("entry", 0.0)) if agent_signals else 0.0

            # 4) Dynamic TP/SL calculation via AdaptiveTPSL
            tpsl_result = await self.tpsl_agent.calculate_tp_sl(
                symbol=symbol,
                entry=entry,
                score=score,
                liquidity=liquidity,
                whale_pressure=whale_pressure,
                user_context=user_context,
                audit_details=audit_details,
            )
            take_profit = float(tpsl_result.get("tp", 0.0))
            stop_loss = float(tpsl_result.get("sl", 0.0))

            # 5) Flavor & caption
            flavor = self._choose_flavor(avg, user_context)
            caption = await self._generate_caption(symbol, {"avg": avg, "depth_adj": depth_adj, "tp": take_profit, "sl": stop_loss})

            # 6) Provenance
            prov_hash = self._compute_provenance(
                symbol, weighted_scores, {"depth_adj": depth_adj}, flavor, score, risk, entry, take_profit, stop_loss
            )

            signal = AlphaSignal(
                symbol=symbol,
                trigger_sources=weighted_scores,
                meta_features={"trusts": trust_map, **ob, "audit": audit_details},
                flavor=flavor,
                score=round(score, 4),
                risk=round(risk, 4),
                entry=round(entry, 6),
                caption=caption,
                provenance_hash=prov_hash,
                timestamp=int(start.timestamp()),
                take_profit=take_profit,
                stop_loss=stop_loss,
                audit_passed=audit_passed,
                audit_details=audit_details,
            )

            SIGNAL_SCORE.labels(symbol=symbol).set(score)
            SIGNAL_LATENCY.observe((datetime.utcnow() - start).total_seconds())

            # 7) Emit event for downstream hooks (execution, logging, telegram)
            await fire_plugin_event("alpha_signal", signal.dict())
            return signal

        except Exception as e:
            SIGNAL_ERRORS.inc()
            logger.error(f"[AlphaSignalAgent] evaluate error for {symbol}: {e}")
            raise

# singleton
alpha_signal_agent = AlphaSignalAgent()
