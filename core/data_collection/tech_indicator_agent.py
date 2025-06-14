# ────────────────────────────────────────────────────────────────────────────────
#  Nova Pulse ALPHA – Technical‑Indicator Agent 2.0
#  -----------------------------------------------------------------------------
#  • Live OHLCV pull (DexScreener "chart" endpoint ‑ 5‑minute candles)
#  • Async/await end‑to‑end – zero blocking I/O
#  • Pluggable scoring via `register_tech_ind_plugin()`
#  • Full Prometheus instrumentation, audit‑hash & DLQ replay
#  • No placeholders ‑ every data‑point is fetched live from main‑net Solana DEXs
# ────────────────────────────────────────────────────────────────────────────────

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional

import httpx
import pandas as pd
import pandas_ta as ta
from prometheus_client import Counter, Histogram

from utils.config import Config

logger = logging.getLogger("nova.tech_indicator_agent")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Prometheus metrics
# ────────────────────────────────────────────────────────────────────────────────
TIND_CALLS        = Counter("tech_ind_calls_total", "Total technical‑indicator agent invocations")
TIND_ERRORS       = Counter("tech_ind_errors_total", "Tech‑indicator failures")
TIND_LATENCY      = Histogram("tech_ind_latency_seconds", "Latency of TA analysis")
TIND_DLQ          = Counter("tech_ind_dlq_total", "Tech‑indicator DLQ events")
TIND_ALERTS       = Counter("tech_ind_alerts_total", "High‑confidence TA alerts")

# ────────────────────────────────────────────────────────────────────────────────
# Event / plug‑in registry
# ────────────────────────────────────────────────────────────────────────────────
_tech_ind_plugins: dict[str, Callable] = {}
_event_hooks: list[Callable[[str, dict[str, Any]], asyncio.Future]] = []
_failed_dlq: list[dict[str, Any]] = []


def register_plugin(name: str, fn: Callable):
    _tech_ind_plugins[name] = fn

def register_event_hook(fn: Callable[[str, dict[str, Any]], asyncio.Future]):
    _event_hooks.append(fn)

async def _fire_event(ev: str, payload: dict[str, Any]):
    for hook in _event_hooks:
        try:
            await hook(ev, payload)
        except Exception as exc:  # noqa: BLE001 – we must not let hooks crash core
            logger.error("[TechInd] hook %s failed: %s", getattr(hook, "__name__", "?"), exc)

# ────────────────────────────────────────────────────────────────────────────────
# Helper: SHA‑256 provenance hash
# ────────────────────────────────────────────────────────────────────────────────

def _hash(symbol: str, result: dict[str, Any]) -> str:
    h = hashlib.sha256()
    h.update(symbol.encode())
    h.update(json.dumps(result, sort_keys=True).encode())
    return h.hexdigest()

# ────────────────────────────────────────────────────────────────────────────────
# Main class
# ────────────────────────────────────────────────────────────────────────────────
class TechIndicatorAgent:
    """Live TA engine pulling 5‑minute OHLCV from DexScreener."""

    _CHART_URL = "https://api.dexscreener.com/latest/dex/chart/solana/{pair_id}?from={start}&to={end}&interval=5m"

    def __init__(self, cfg: Optional[Config] = None):
        self.cfg = cfg or Config()
        # user supplies the *DexScreener pair ID* (ex: "SOL_USDC") when calling analyse()

    # ── Data fetch ──────────────────────────────────────────────────────────
    async def _fetch_candles(self, pair_id: str, lookback: int = 500) -> pd.DataFrame | None:
        """Fetch *lookback* 5‑minute candles for `pair_id`. Returns DataFrame or None."""
        end_ts = int(datetime.utcnow().timestamp())
        start_ts = end_ts - lookback * 300  # 300 sec per 5‑m candle
        url = self._CHART_URL.format(pair_id=pair_id, start=start_ts, end=end_ts)
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.json().get("candles", [])
                if not data:
                    return None
                df = pd.DataFrame(data)
                df.rename(columns={"t": "ts", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}, inplace=True)
                df["close"] = pd.to_numeric(df["close"], errors="coerce")
                return df.dropna()
        except Exception as exc:
            TIND_ERRORS.inc()
            logger.error("[TechInd] fetch_candles error %s: %s", pair_id, exc)
            return None

    # ── Public analyse entrypoint ───────────────────────────────────────────
    async def analyse(self, pair_id: str, *, plugin: str = "default", simulation: bool = False, **kw) -> dict[str, Any]:
        """Return score/meta for *pair_id* (DexScreener pair ID)."""
        TIND_CALLS.inc()
        try:
            async with TIND_LATENCY.time():
                fn = _tech_ind_plugins.get(plugin, self._default_analysis)
                result = await fn(pair_id, simulation=simulation, **kw)
                payload = {
                    "pair_id": pair_id,
                    "result": result,
                    "simulation": simulation,
                    "hash": _hash(pair_id, result),
                }
                await _fire_event("tech_ind_analyzed", payload)
                if result.get("score", 0) >= 0.85:
                    TIND_ALERTS.inc()
                    await _fire_event("tech_ind_alert", payload)
                return result
        except Exception as exc:
            TIND_ERRORS.inc(); TIND_DLQ.inc()
            _failed_dlq.append({"pair_id": pair_id, "error": str(exc)})
            await _fire_event("tech_ind_error", {"pair_id": pair_id, "error": str(exc)})
            logger.error("[TechInd] analyse error for %s: %s", pair_id, exc)
            return {"pair_id": pair_id, "score": 0.0, "meta": {"error": str(exc)}}

    # ── Default TA strategy ────────────────────────────────────────────────
    async def _default_analysis(self, pair_id: str, *, simulation: bool = False, **_) -> dict[str, Any]:
        df = await self._fetch_candles(pair_id)
        if df is None or len(df) < 120:  # need at least 10 hours of data
            return {"score": 0.0, "meta": {"error": "insufficient data"}}

        # compute indicators (vectorised via pandas‑ta)
        df.ta.rsi(length=14, append=True)
        macd = ta.macd(df["close"], fast=12, slow=26, signal=9)
        df["macd_hist"] = macd["MACDh_12_26_9"]
        df.ta.sma(length=20, append=True)
        bb = ta.bbands(df["close"], length=20)
        df["bb_low"], df["bb_high"] = bb["BBL_20_2.0"], bb["BBU_20_2.0"]

        last = df.iloc[-1]
        rsi = float(last["RSI_14"])
        macd_hist = float(last["macd_hist"])
        close = float(last["close"])
        upper, lower = float(last["bb_high"]), float(last["bb_low"])
        sma20 = float(last["SMA_20"])

        score = 0.0; rationale: list[str] = []
        if rsi < 30:                     score += 0.25; rationale.append("RSI oversold")
        if macd_hist > 0:                score += 0.25; rationale.append("MACD bullish")
        if close < lower:                score += 0.25; rationale.append("Bollinger pierce")
        if close > sma20:                score += 0.25; rationale.append("Above SMA20")
        if simulation:                   score *= 0.97

        return {
            "score": round(score, 2),
            "entry": close,
            "meta": {
                "rsi": rsi,
                "macd_hist": macd_hist,
                "bb": (lower, upper),
                "sma20": sma20,
                "rationale": rationale,
            },
        }

    # ── DLQ helpers ─────────────────────────────────────────────────────────
    async def replay_dlq(self):
        for item in _failed_dlq[:]:
            try:
                await self.analyse(item["pair_id"])
                _failed_dlq.remove(item)
            except Exception:  # keep in DLQ
                continue

    # exposed for monitoring
    def dlq(self) -> list[dict[str, Any]]:
        return _failed_dlq.copy()


# Register default plugin on import
register_plugin("default", TechIndicatorAgent._default_analysis)
