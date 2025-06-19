import asyncio
import logging
import time
import json
from typing import List, Dict, Any

import aiohttp
import aioredis
import numpy as np
from pydantic import BaseModel
from prometheus_client import Counter, Histogram
from core.utils.config import Config

logger = logging.getLogger("risk_signals")

# ─── Prometheus Metrics ──────────────────────────────
RISK_REQUESTS = Counter("risk_signals_requests_total", "Total risk signal requests")
RISK_LATENCY = Histogram("risk_signals_latency_seconds", "Latency of risk assessment operations")
RISK_ERRORS = Counter("risk_signals_errors_total", "Total errors in risk signal generation")

# ─── External Sources/Constants ─────────────────────
JITO_SNIPER_DB = "https://raw.githubusercontent.com/jito-foundation/jito-snipers/main/snipers.json"
TRAP_KEYWORDS = ["honeypot", "snipe", "rug", "trap", "scam", "exit"]
RUGCHECK_API = "https://api.rugcheck.xyz/v1/token/"

# ─── Risk Data Model ────────────────────────────────
class RiskMetrics(BaseModel):
    symbol: str
    volatility: float
    var_95: float
    liquidity_risk: float
    timestamp: int
    sniper_flag: bool = False
    sniper_reason: str = ""
    trap_flag: bool = False
    trap_reason: str = ""
    rugcheck_score: float = None
    meta_risk_score: float = None

# ─── Core Risk Engine ───────────────────────────────
class RiskSignals:
    def __init__(self, redis_url: str, cmc_api_key: str, redis_ttl: int = 300, var_window: int = 252, vol_window: int = 30):
        """
        Initialize the RiskSignals engine with configuration settings.
        
        :param redis_url: URL for the Redis cache.
        :param cmc_api_key: CoinMarketCap API key.
        :param redis_ttl: Time-to-live for Redis cache (default: 300 seconds).
        :param var_window: The window size (in days) for Value at Risk calculation (default: 252).
        :param vol_window: The window size (in days) for volatility calculation (default: 30).
        """
        self.redis_url = redis_url
        self.redis_ttl = redis_ttl
        self.cmc_api_key = cmc_api_key
        self.var_window = var_window
        self.vol_window = vol_window
        self.redis = None

    async def connect_redis(self):
        """Connect to Redis cache."""
        if self.redis is None:
            self.redis = await aioredis.from_url(self.redis_url, encoding="utf8", decode_responses=True)
            logger.info("[Risk] Connected to Redis.")

    async def check_jito_sniper(self, mint: str) -> Dict[str, Any]:
        """Check if the mint is flagged by Jito sniper DB."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(JITO_SNIPER_DB, timeout=10) as resp:
                    if resp.status != 200:
                        logger.warning(f"[Risk] Jito DB fetch error: {resp.status}")
                        return {"flagged": False, "reason": "unreachable"}
                    snipers = await resp.json()
                    if mint in snipers:
                        return {"flagged": True, "reason": "jito-sniper"}
        except Exception as e:
            logger.error(f"[Risk] Error during sniper check: {e}")
        return {"flagged": False}

    @staticmethod
    def symbol_is_trappy(symbol: str) -> bool:
        """Check if the symbol name contains trap-related keywords."""
        symbol = symbol.lower()
        return any(kw in symbol for kw in TRAP_KEYWORDS)

    async def fetch_market_data(self, symbol: str) -> List[float]:
        """Fetch market data (e.g., historical prices) for a symbol."""
        raise NotImplementedError("fetch_market_data must be wired to live pricing source (e.g., CMC, DEX aggregator)")

    def compute_var(self, prices: List[float], alpha: float = 0.05) -> float:
        """Calculate Value at Risk (VaR) for the symbol."""
        returns = np.diff(prices) / prices[:-1]
        return np.percentile(returns, 100 * alpha)

    def compute_volatility(self, prices: List[float]) -> float:
        """Calculate the volatility of the symbol."""
        returns = np.diff(prices) / prices[:-1]
        return np.std(returns) * np.sqrt(self.vol_window)

    async def compute_liquidity_risk(self, symbol: str) -> float:
        """Calculate liquidity risk for the symbol."""
        raise NotImplementedError("compute_liquidity_risk must be connected to a DEX/CMC liquidity API")

    async def rugcheck_score(self, mint: str) -> float:
        """Fetch the Rugcheck score for a symbol."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(RUGCHECK_API + mint, timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        return float(data.get("score", 0))
        except Exception as e:
            logger.warning(f"[Risk] Rugcheck error: {e}")
        return None

    async def meta_risk_ml_score(self, features: Dict[str, Any]) -> float:
        """Compute a meta risk score based on multiple risk features."""
        base = 0.5
        if features.get("sniper_flag"):
            base += 0.25
        if features.get("trap_flag"):
            base += 0.1
        if features.get("volatility", 0) > 2.0:
            base += 0.1
        return min(base, 1.0)

    async def get_risk_metrics(self, symbol: str, mint: str) -> RiskMetrics:
        """
        Generate the full risk metrics for a symbol by performing various risk checks.
        
        :param symbol: The symbol to assess (e.g., "BTC").
        :param mint: The mint address of the token.
        :return: A RiskMetrics object with the computed risk metrics.
        """
        RISK_REQUESTS.inc()
        async with RISK_LATENCY.time():
            await self.connect_redis()
            cache_key = f"risk:{symbol}:{mint}"
            cached = await self.redis.get(cache_key)
            if cached:
                return RiskMetrics.parse_raw(cached)

            sniper = await self.check_jito_sniper(mint)
            trap = self.symbol_is_trappy(symbol)
            try:
                prices = await self.fetch_market_data(symbol)
                volatility = self.compute_volatility(prices)
                var_95 = self.compute_var(prices)
                liquidity_risk = await self.compute_liquidity_risk(symbol)
                rug_score = await self.rugcheck_score(mint)
                timestamp = int(time.time())
                features = {
                    "sniper_flag": sniper["flagged"],
                    "trap_flag": trap,
                    "volatility": volatility,
                }
                meta_risk = await self.meta_risk_ml_score(features)

                result = RiskMetrics(
                    symbol=symbol,
                    volatility=volatility,
                    var_95=var_95,
                    liquidity_risk=liquidity_risk,
                    timestamp=timestamp,
                    sniper_flag=sniper["flagged"],
                    sniper_reason=sniper.get("reason", ""),
                    trap_flag=trap,
                    trap_reason="trappy-symbol" if trap else "",
                    rugcheck_score=rug_score,
                    meta_risk_score=meta_risk
                )
                await self.redis.set(cache_key, result.json(), ex=self.redis_ttl)
                return result
            except Exception as e:
                RISK_ERRORS.inc()
                logger.error(f"[Risk] Failed to compute risk metrics for {symbol}: {e}")
                raise
