# core/utils/token_classifier.py

import logging
import time
from typing import Any, Dict, Optional

import httpx
import redis.asyncio as redis_client
import openai
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_classifier")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
CLASS_REQS = Counter(
    "token_classifier_requests_total", "Total token classification requests"
)
CLASS_LATENCY = Histogram(
    "token_classifier_latency_seconds", "Latency of token classification operations"
)
CLASS_ERRORS = Counter(
    "token_classifier_errors_total", "Errors in token classification"
)

# ────────────────────────────────────────────────────────────────────────────────
# Data Model
# ────────────────────────────────────────────────────────────────────────────────
class TokenClassification(BaseModel):
    symbol: str
    classification: str = Field(..., description="Assigned token type")
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: Optional[str] = Field(None, description="Explanation for classification")
    timestamp: int

# ────────────────────────────────────────────────────────────────────────────────
# Token Classifier
# ────────────────────────────────────────────────────────────────────────────────
class TokenClassifier:
    """
    Async classifier for tokens using heuristics, CoinMarketCap, and optional LLM.
    Caches results in Redis and emits Prometheus metrics.
    """
    def __init__(self, cache_ttl: int = 86400) -> None:
        self.redis_url = config.REDIS_URL
        self.cmc_key = config.CMC_API_KEY
        self.openai_key = config.OPENAI_API_KEY
        self.cache_ttl = cache_ttl
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=10)
        if self.openai_key:
            openai.api_key = self.openai_key

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TokenClassifier] Connected to Redis")
        except Exception as e:
            logger.error(f"[TokenClassifier] Redis connection failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        await self.http.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[TokenClassifier] Shutdown complete")

    async def classify_token(self, mint: str, chain: str = "solana") -> TokenClassification:
        CLASS_REQS.inc()
        start = time.time()
        cache_key = f"classify:{chain}:{mint}"
        try:
            await self.connect()
            # Cache lookup
            if self.redis:
                raw = await self.redis.get(cache_key)
                if raw:
                    result = TokenClassification.parse_raw(raw)
                    CLASS_LATENCY.observe(time.time() - start)
                    return result
            # Fetch metadata and score
            meta, score = await self._fetch_metadata_and_score(mint, chain)
            # Generate rationale if enabled
            rationale = None
            if self.openai_key:
                rationale = await self._llm_rationale(meta)
            classification = meta.get("classification", "other")
            result = TokenClassification(
                symbol=meta.get("symbol", mint[:6]),
                classification=classification,
                confidence=score,
                rationale=rationale,
                timestamp=int(time.time()),
            )
            # Cache result
            if self.redis:
                await self.redis.set(cache_key, result.json(), ex=self.cache_ttl)
            CLASS_LATENCY.observe(time.time() - start)
            return result
        except Exception as e:
            CLASS_ERRORS.inc()
            logger.error(f"[TokenClassifier] classify_token error for {mint}: {e}")
            raise

    async def _fetch_metadata_and_score(self, mint: str, chain: str) -> (Dict[str, Any], float):
        """
        Heuristic + CMC-based classification.
        """
        score = 0.5
        meta: Dict[str, Any] = {"symbol": mint[:6], "classification": "other"}
        try:
            url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
            headers = {"X-CMC_PRO_API_KEY": self.cmc_key}
            params = {"id": mint}
            async with self.http.get(url, headers=headers, params=params) as resp:
                resp.raise_for_status()
                data = resp.json().get("data", {}).get(str(mint), {})
            # Apply heuristics
            classification = self._heuristic_class(data)
            score = 0.9 if classification == "stablecoin" else 0.7
            meta.update({
                "symbol": data.get("symbol", meta["symbol"]),
                "classification": classification,
                "name": data.get("name"),
                "description": data.get("description"),
            })
        except Exception:
            # Leave defaults on failure
            pass
        return meta, score

    def _heuristic_class(self, data: Dict[str, Any]) -> str:
        name = (data.get("name") or "").lower()
        symbol = (data.get("symbol") or "").lower()
        desc = (data.get("description") or "").lower()
        if any(x in symbol for x in ["usd", "usdt", "usdc"]) or "stable" in name:
            return "stablecoin"
        if "governance" in desc:
            return "governance"
        if "utility" in desc:
            return "utility"
        if "meme" in desc or any(x in symbol for x in ["dog", "cat"]):
            return "memecoin"
        if "security" in desc:
            return "security"
        return "other"

    async def _llm_rationale(self, meta: Dict[str, Any]) -> str:
        try:
            prompt = (
                f"Why is the token with symbol {meta.get('symbol')} classified as {meta.get('classification')}?\n"
                f"Description: {meta.get('description', 'N/A')}"
            )
            resp = await openai.ChatCompletion.acreate(
                model="gpt-4-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=100,
                temperature=0.3,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"[TokenClassifier] LLM rationale failed: {e}")
            return ""
