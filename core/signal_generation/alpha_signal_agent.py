# core/signal_generation/alpha_signal_agent.py

import asyncio
import hashlib
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

import openai
import redis.asyncio as aioredis
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram, Gauge

from core.utils.config import config
from core.utils.trade_memory import TradeMemory
from core.signal_generation.reinforcement_tuner import ReinforcementTuner
from core.signal_generation.reinforcement_learning import RLTrader
from core.data_collection.orderbook_overlay import get_orderbook_imbalance
from core.utils.plugin_manager import fire_plugin_event
from core.utils.token_auditor import TokenAuditor
from core.utils.token_guardian import TokenGuardian
from core.agents.adaptive_tp_sl import AdaptiveTPSL

logger = logging.getLogger("nova.alpha_signal_agent")
logger.setLevel(logging.INFO)

SIGNAL_TRIGGERS = Counter("alpha_signal_triggers_total", "Total alpha signals triggered")
SIGNAL_ERRORS   = Counter("alpha_signal_errors_total",   "Total alpha signal errors")
SIGNAL_LATENCY  = Histogram("alpha_signal_latency_seconds", "Alpha signal latency")
SIGNAL_SCORE    = Gauge("alpha_signal_score", "Last alpha signal score", ["symbol"])

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

class AlphaSignalAgent:
    def __init__(
        self,
        redis_url: str = config.REDIS_URL,
        openai_key: str = config.OPENAI_API_KEY,
        symbols: Optional[List[str]] = None,
    ):
        openai.api_key = openai_key
        self.redis: Optional[aioredis.Redis] = None
        self.memory = TradeMemory(config)
        self.rl_tuner = ReinforcementTuner()
        self.symbols = symbols or []
        self._cap_lock = asyncio.Lock()

        self.token_auditor  = TokenAuditor()
        self.token_guardian = TokenGuardian
        self.tpsl_agent     = AdaptiveTPSL(memory=self.memory, rl_agent=self.rl_tuner)

    async def connect(self):
        if not self.redis:
            self.redis = await aioredis.from_url(config.REDIS_URL, decode_responses=True)
            logger.info("[AlphaSignalAgent] Redis connected")

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            logger.info("[AlphaSignalAgent] Redis disconnected")

    async def _gen_caption(self, symbol: str, meta: Dict[str, Any]) -> str:
        prompt = f"Craft a concise trading signal for {symbol} with context {meta}."
        async with self._cap_lock:
            try:
                resp = await openai.ChatCompletion.acreate(
                    model="gpt-3.5-turbo",
                    messages=[{"role":"user","content":prompt}],
                    max_tokens=32, temperature=0.7
                )
                return resp.choices[0].message.content.strip()
            except Exception as e:
                logger.warning(f"[AlphaSignalAgent] Caption error: {e}")
                return f"{symbol} signal 🚀"

    def _compute_prov(self, symbol: str, scores: Dict[str,float], meta: Dict[str,Any], flavor:str, score:float, risk:float, entry:float, tp:float, sl:float) -> str:
        blob = f"{symbol}{scores}{meta}{flavor}{score}{risk}{entry}{tp}{sl}"
        return hashlib.sha256(blob.encode()).hexdigest()

    def _choose_flavor(self, avg: float, ctx: Optional[Dict[str,Any]]) -> str:
        if ctx and ctx.get("risk") == "degen": return "Degen"
        if avg > 0.85: return "Safe"
        if avg < 0.3:  return "Degen"
        return "Moderate"

    async def _run_audit(self, symbol: str) -> (bool, Dict[str, Any]):
        details = {}
        # TokenAuditor
        try:
            res = await self.token_auditor.audit_token(symbol)
            details["TokenAuditor"] = res.dict()
            if res.blacklist:
                return False, details
        except Exception as e:
            details["TokenAuditor"] = {"error": str(e)}
            return False, details

        # TokenGuardian
        try:
            res = await self.token_guardian.check(symbol)
            details["TokenGuardian"] = res
            if not res.get("passed", True):
                return False, details
        except Exception as e:
            details["TokenGuardian"] = {"error": str(e)}
            return False, details

        return True, details

    async def evaluate(
        self,
        symbol: str,
        agent_signals: List[Dict[str, Any]],
        user_context: Optional[Dict[str, Any]] = None,
    ) -> AlphaSignal:
        SIGNAL_TRIGGERS.inc()
        start = datetime.utcnow()
        try:
            # audit
            ok, audit_det = await self._run_audit(symbol)
            if not ok:
                raise RuntimeError(f"Audit failed: {audit_det}")

            # weight signals
            weighted, trusts = {}, {}
            mem_rate = await asyncio.get_event_loop().run_in_executor(
                None, self.memory.win_rate, symbol
            )
            for sig in agent_signals:
                src  = sig["source"]
                base = float(sig.get("score",0))
                tr   = self.rl_tuner.get_trust_score(src, symbol)
                w    = (tr + mem_rate)/2
                weighted[src] = base * w
                trusts[src]   = w

            # get orderbook
            imbalance, ob_meta = await get_orderbook_imbalance(symbol)
            depth_adj = 1.0 if imbalance>=0 else 0.9

            avg   = sum(weighted.values())/max(len(weighted),1)
            score = avg * depth_adj
            risk  = 1 - min(trusts.values()) if trusts else 1
            entry = float(agent_signals[0].get("entry",0)) if agent_signals else 0.0

            # TP/SL
            tpsl  = await self.tpsl_agent.compute(symbol, entry, {"imbalance":imbalance}, user_context)
            tp, sl = float(tpsl["tp"]), float(tpsl["sl"])

            # flavor & caption
            flavor = self._choose_flavor(avg, user_context)
            caption= await self._gen_caption(symbol, {"avg":avg})

            prov = self._compute_prov(symbol, weighted, ob_meta, flavor, score, risk, entry, tp, sl)
            sig = AlphaSignal(
                symbol=symbol,
                trigger_sources=weighted,
                meta_features={**{"depth_adj":depth_adj}, **ob_meta, **audit_det},
                flavor=flavor,
                score=round(score,4),
                risk=round(risk,4),
                entry=round(entry,6),
                caption=caption,
                provenance_hash=prov,
                timestamp=int(start.timestamp()),
                take_profit=tp,
                stop_loss=sl,
                audit_passed=ok,
                audit_details=audit_det,
            )

            SIGNAL_SCORE.labels(symbol=symbol).set(sig.score)
            SIGNAL_LATENCY.observe((datetime.utcnow() - start).total_seconds())
            await fire_plugin_event("alpha_signal", sig.dict())
            return sig

        except Exception as e:
            SIGNAL_ERRORS.inc()
            logger.error(f"[AlphaSignalAgent] evaluate error for {symbol}: {e}", exc_info=True)
            raise

# singleton instance
alpha_signal_agent = AlphaSignalAgent()
