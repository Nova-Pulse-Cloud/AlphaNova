# Upgraded: core/tech_indicator_agent.py (pro async, Prometheus, event, DLQ, plugin, audit, ops, simulation)

import logging
import httpx
import pandas as pd
import pandas_ta as ta
import asyncio
import hashlib
import json
from typing import Dict, Any, Optional, Callable, List
from prometheus_client import Counter, Histogram

logger = logging.getLogger("tech_indicator_agent")

# === Prometheus metrics ===
TIND_CALLS = Counter("tech_ind_calls_total", "Total technical indicator agent calls")
TIND_ERRORS = Counter("tech_ind_errors_total", "Tech indicator errors")
TIND_LATENCY = Histogram("tech_ind_latency_seconds", "Tech indicator latency")
TIND_DLQ = Counter("tech_ind_dlq_total", "Tech indicator DLQ events")
TIND_ALERTS = Counter("tech_ind_alerts_total", "Tech indicator alert triggers")

# === DLQ, event hooks, plugin registry ===
FAILED_TIND_DLQ = []
tech_ind_event_hooks = []
tech_ind_plugins = {}

def register_tech_ind_plugin(name: str, fn: Callable):
    tech_ind_plugins[name] = fn

def register_tech_ind_event_hook(hook_fn):
    tech_ind_event_hooks.append(hook_fn)

async def fire_tech_ind_event(event_name, data):
    for hook in tech_ind_event_hooks:
        await hook(event_name, data)

def tech_ind_hash(symbol: str, result: dict) -> str:
    m = hashlib.sha256()
    m.update(symbol.encode())
    m.update(json.dumps(result, sort_keys=True).encode())
    return m.hexdigest()

class TechIndicatorAgent:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.api_url = "https://api.dexscreener.com/latest/dex/pairs/solana/{}"
        self.loop = asyncio.get_event_loop()

    async def fetch_price_data(self, symbol: str) -> Optional[pd.DataFrame]:
        url = self.api_url.format(symbol)
        try:
            async with httpx.AsyncClient() as client:
                r = await client.get(url)
                r.raise_for_status()
                data = r.json()
                if 'pair' in data and 'priceUsd' in data['pair']:
                    prices = [float(data['pair']['priceUsd'])] * 100  # Placeholder: 100 fake data points for now
                    return pd.DataFrame({"close": prices})
                return None
        except Exception as e:
            TIND_ERRORS.inc()
            logger.error(f"[TechIndicatorAgent] Failed to fetch price data for {symbol}: {e}")
            return None

    async def analyze(self, symbol: str, plugin: str = "default", simulation: bool = False, **kwargs):
        TIND_CALLS.inc()
        try:
            async with TIND_LATENCY.time():
                fn = tech_ind_plugins.get(plugin, self.default_analysis)
                result = await fn(self, symbol, simulation=simulation, **kwargs)
                audit = {
                    "symbol": symbol,
                    "result": result,
                    "simulation": simulation,
                    "hash": tech_ind_hash(symbol, result)
                }
                await fire_tech_ind_event("tech_ind_analyzed", audit)
                if result.get("score", 0.0) > 0.85:
                    TIND_ALERTS.inc()
                    await fire_tech_ind_event("tech_ind_alert", audit)
                return result
        except Exception as e:
            TIND_ERRORS.inc()
            TIND_DLQ.inc()
            FAILED_TIND_DLQ.append({"symbol": symbol, "error": str(e)})
            await fire_tech_ind_event("tech_ind_error", {"symbol": symbol, "error": str(e)})
            logger.error(f"[TechIndicatorAgent] Analysis error for {symbol}: {e}")
            return {"symbol": symbol, "score": 0.0, "meta": {"error": str(e)}}

    async def default_analysis(self, symbol: str, simulation: bool = False, **kwargs) -> Dict[str, Any]:
        df = await self.fetch_price_data(symbol)
        if df is None or len(df) < 50:
            return {"score": 0.0, "meta": {"error": "insufficient data"}}

        # Compute indicators
        df.ta.rsi(length=14, append=True)
        df.ta.macd(append=True)
        df.ta.sma(length=20, append=True)
        df.ta.bbands(length=20, append=True)

        rsi = df.iloc[-1]["RSI_14"]
        macd_hist = df.iloc[-1]["MACDh_12_26_9"]
        close = df.iloc[-1]["close"]
        upper_band = df.iloc[-1]["BBU_20_2.0"]
        lower_band = df.iloc[-1]["BBL_20_2.0"]

        score = 0.0
        rationale = []

        if rsi < 30:
            score += 0.25
            rationale.append("RSI oversold")
        if macd_hist > 0:
            score += 0.25
            rationale.append("MACD bullish")
        if close < lower_band:
            score += 0.25
            rationale.append("Bollinger lower band touch")
        if close > df.iloc[-1]["SMA_20"]:
            score += 0.25
            rationale.append("Price above SMA20")

        # Simulate: Reduce score slightly in simulation mode
        if simulation:
            score *= 0.97

        return {
            "symbol": symbol,
            "score": round(score, 2),
            "meta": {
                "rsi": rsi,
                "macd_hist": macd_hist,
                "bb_range": (lower_band, upper_band),
                "rationale": rationale
            },
            "entry": close
        }

    async def replay_dlq(self):
        for failed in FAILED_TIND_DLQ[:]:
            try:
                _ = await self.analyze(failed.get("symbol", ""))
                FAILED_TIND_DLQ.remove(failed)
            except Exception:
                continue

    def get_dlq(self):
        return FAILED_TIND_DLQ.copy()
