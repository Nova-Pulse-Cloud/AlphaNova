# core/utils/token_classifier.py

import asyncio
import logging
import time
from typing import Any, Dict, Optional, List

import httpx
import redis.asyncio as redis_client
import openai
from pydantic import BaseModel, Field
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_classifier")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
CLASS_REQS        = Counter("token_classifier_requests_total",    "Total token classification requests")
CLASS_ERRORS      = Counter("token_classifier_errors_total",      "Errors in token classification")
CACHE_HITS        = Counter("token_classifier_cache_hits_total",  "Cache hits for token classification")
CACHE_MISSES      = Counter("token_classifier_cache_misses_total","Cache misses for token classification")
CLASS_LATENCY     = Histogram("token_classifier_latency_seconds",   "Latency of classification operations")
CMC_LATENCY       = Histogram("token_classifier_cmc_latency_seconds","Latency of CMC metadata fetches")
LLM_LATENCY       = Histogram("token_classifier_llm_latency_seconds","Latency of LLM rationale calls")

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
    Async, Redis-cached token classifier:
    - Heuristic + CMC metadata
    - Optional LLM rationale for low-confidence cases
    - Fully instrumented for latency and cache metrics
    """
    def __init__(
        self,
        cache_ttl: int = 86400,
        llm_conf_threshold: float = 0.8,
    ) -> None:
        self.redis_url = config.REDIS_URL
        self.cmc_key   = config.CMC_API_KEY
        self.openai_key= config.OPENAI_API_KEY
        self.cache_ttl = cache_ttl
        self.llm_conf_threshold = llm_conf_threshold
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=10)
        if self.openai_key:
            openai.api_key = self.openai_key

    async def connect(self) -> None:
        """Initialize Redis connection."""
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            logger.info("[TokenClassifier] Redis connected")
        except Exception as e:
            logger.error(f"[TokenClassifier] Redis connect failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        """Close HTTP and Redis clients."""
        await self.http.aclose()
        if self.redis:
            await self.redis.close()
        logger.info("[TokenClassifier] Shutdown complete")

    async def classify_token(
        self,
        mint: str,
        chain: str = "solana",
    ) -> TokenClassification:
        """Classify a token by mint, with caching and metrics."""
        CLASS_REQS.inc()
        start = time.time()
        cache_key = f"classify:{chain}:{mint}"
        try:
            await self.connect()
            # Cache lookup
            if self.redis:
                with CLASS_LATENCY.time():
                    raw = await self.redis.get(cache_key)
                if raw:
                    CACHE_HITS.inc()
                    result = TokenClassification.parse_raw(raw)
                    return result
            CACHE_MISSES.inc()

            # Fetch metadata + heuristic classification
            meta, score = await self._fetch_metadata_and_score(mint)

            # LLM rationale only for uncertain cases
            rationale: Optional[str] = None
            if self.openai_key and score < self.llm_conf_threshold:
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
            return result

        except Exception as e:
            CLASS_ERRORS.inc()
            logger.error(f"[TokenClassifier] Error classifying {mint}: {e}")
            raise
        finally:
            CLASS_LATENCY.observe(time.time() - start)

    async def _fetch_metadata_and_score(
        self,
        mint: str,
    ) -> (Dict[str, Any], float):
        """Fetch CMC metadata and apply heuristic scoring."""
        score = 0.5
        meta: Dict[str, Any] = {"symbol": mint[:6], "classification": "other"}
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
        headers = {"X-CMC_PRO_API_KEY": self.cmc_key}
        params = {"id": mint}
        try:
            async for attempt in AsyncRetrying(
                stop=stop_after_attempt(2), wait=wait_exponential_jitter(1,3), reraise=True
            ):
                with CMC_LATENCY.time():
                    resp = await self.http.get(url, headers=headers, params=params)
                    resp.raise_for_status()
            data = resp.json().get("data", {}).get(str(mint), {})
            symbol = data.get("symbol", meta["symbol"])
            classification = self._heuristic_class(data)
            score = 0.9 if classification == "stablecoin" else 0.7
            meta.update({
                "symbol": symbol,
                "classification": classification,
                "description": data.get("description"),
            })
        except Exception as e:
            logger.warning(f"[TokenClassifier] CMC fetch failed for {mint}: {e}")
        return meta, score

    def _heuristic_class(self, data: Dict[str, Any]) -> str:
        """Assign a classification based on metadata heuristics."""
        name = (data.get("name") or "").lower()
        symbol = (data.get("symbol") or "").lower()
        desc = (data.get("description") or "").lower()
        if any(x in symbol for x in ["usd","usdt","usdc"]) or "stable" in name:
            return "stablecoin"
        if "governance" in desc:
            return "governance"
        if "utility" in desc:
            return "utility"
        if "meme" in desc or any(x in symbol for x in ["dog","cat"]):
            return "memecoin"
        if "security" in desc:
            return "security"
        return "other"

    async def _llm_rationale(self, meta: Dict[str, Any]) -> str:
        """Generate classification rationale via OpenAI LLM."""
        prompt = (
            f"Explain why {meta.get('symbol')} is classified as {meta.get('classification')}. "
            f"Description: {meta.get('description','N/A')}"
        )
        try:
            with LLM_LATENCY.time():
                resp = await openai.ChatCompletion.acreate(
                    model="gpt-4-turbo",
                    messages=[{"role":"user","content":prompt}],
                    max_tokens=100,
                    temperature=0.4,
                )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"[TokenClassifier] LLM rationale error: {e}")
            return ""
