import os
import asyncio
import logging
import time
from typing import List, Optional, Dict, Any

import httpx
import aioredis
import openai
from pydantic import BaseModel, Field
from prometheus_client import Counter, Histogram
from utils.config import Config

logger = logging.getLogger("token_classifier")

# Prometheus metrics
CLASSIFICATION_REQUESTS = Counter(
    "token_classification_requests_total", "Total token classification requests"
)
CLASSIFICATION_LATENCY = Histogram(
    "token_classification_latency_seconds", "Latency of token classification operations"
)

# Token types
token_types = ["stablecoin", "governance", "utility", "memecoin", "security", "other"]

class TokenClassification(BaseModel):
    symbol: str
    classification: str = Field(..., description="Assigned token type")
    confidence: float = Field(..., ge=0.0, le=1.0)
    rationale: Optional[str] = Field(None, description="Explanation for classification")
    timestamp: int

class TokenClassifier:
    """
    Modular, async, LLM-augmented classifier for tokens (Solana, ETH, and future chains).
    Uses Redis for cache, Prometheus for metrics, and LLM for explanations.
    """
    def __init__(
        self,
        redis_url: str,
        cmc_api_key: str,
        openai_api_key: Optional[str] = None,
        cache_ttl: int = 86400,
    ):
        """
        Initializes the TokenClassifier.

        :param redis_url: URL for Redis cache.
        :param cmc_api_key: CoinMarketCap API key.
        :param openai_api_key: Optional OpenAI API key for generating explanations.
        :param cache_ttl: Time-to-live for Redis cache (default: 86400 seconds).
        """
        self.redis_url = redis_url
        self.cmc_api_key = cmc_api_key
        self.cache_ttl = cache_ttl
        self.redis = None
        self.client = httpx.AsyncClient()
        self.openai_enabled = bool(openai_api_key)
        if openai_api_key:
            openai.api_key = openai_api_key

    async def connect(self):
        """Connect to Redis."""
        if not self.redis:
            self.redis = await aioredis.from_url(self.redis_url)
            logger.info("TokenClassifier connected to Redis.")

    async def classify_token(self, mint: str, chain: str = "solana") -> TokenClassification:
        """
        Classifies a token with heuristics, CoinMarketCap metadata, and optional LLM rationale.
        Results are cached in Redis.

        :param mint: The mint address of the token to classify.
        :param chain: The blockchain the token resides on (default: Solana).
        :return: TokenClassification object containing classification details.
        """
        CLASSIFICATION_REQUESTS.inc()
        async with CLASSIFICATION_LATENCY.time():
            await self.connect()
            cache_key = f"classify:{chain}:{mint}"
            cached = await self.redis.get(cache_key)
            if cached:
                return TokenClassification.parse_raw(cached)

            meta, confidence = await self._fetch_metadata_and_score(mint, chain)
            rationale = await self._llm_rationale(meta) if self.openai_enabled else None

            result = TokenClassification(
                symbol=meta.get("symbol", mint[:6]),
                classification=meta.get("classification", "other"),
                confidence=confidence,
                rationale=rationale,
                timestamp=int(time.time()),
            )
            await self.redis.set(cache_key, result.json(), ex=self.cache_ttl)
            return result

    async def _fetch_metadata_and_score(self, mint: str, chain: str = "solana") -> (Dict, float):
        """
        Heuristic + API-based token classification and scoring.

        :param mint: The mint address of the token.
        :param chain: The blockchain the token resides on (default: Solana).
        :return: A tuple with metadata and classification confidence score.
        """
        score = 0.7
        meta = {"symbol": mint[:6], "classification": "other"}
        try:
            if chain == "solana":
                cmc_url = f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/info"
                headers = {"X-CMC_PRO_API_KEY": self.cmc_api_key}
                async with httpx.AsyncClient() as session:
                    r = await session.get(f"{cmc_url}?address={mint}", headers=headers, timeout=10)
                    if r.status_code == 200:
                        data = r.json().get("data", {}).get(mint)
                        if data:
                            meta.update({
                                "symbol": data.get("symbol", mint[:6]),
                                "classification": self._heuristic_class(data),
                                "name": data.get("name"),
                                "description": data.get("description"),
                                "logo": data.get("logo"),
                            })
                            score = 0.95 if meta["classification"] == "stablecoin" else 0.8
            # Expand: ETH, Sui, etc.

        except Exception as e:
            logger.warning(f"[TokenClassifier] Metadata fetch error: {e}")

        return meta, score

    def _heuristic_class(self, data: Dict[str, Any]) -> str:
        """
        Infer classification using heuristics based on token metadata.

        :param data: Metadata for the token.
        :return: A classification string (e.g., 'stablecoin', 'governance').
        """
        name = (data.get("name") or "").lower()
        symbol = (data.get("symbol") or "").lower()
        desc = (data.get("description") or "").lower()
        if "usd" in symbol or "stable" in name or "pegged" in desc:
            return "stablecoin"
        if "governance" in desc or "vote" in desc:
            return "governance"
        if "utility" in desc or "access" in desc:
            return "utility"
        if "meme" in desc or "dog" in symbol or "cat" in symbol:
            return "memecoin"
        if "security" in desc:
            return "security"
        return "other"

    async def _llm_rationale(self, meta: Dict[str, Any]) -> str:
        """
        Generate an explanation for the assigned classification using OpenAI.

        :param meta: Metadata for the token.
        :return: A text explanation for the token's classification.
        """
        try:
            prompt = (
                f"Given the following token metadata, explain why it is classified as '{meta.get('classification', 'other')}':\n"
                f"Name: {meta.get('name', 'N/A')}\n"
                f"Symbol: {meta.get('symbol', 'N/A')}\n"
                f"Description: {meta.get('description', 'N/A')}\n"
                f"Logo: {meta.get('logo', 'N/A')}\n"
            )
            resp = await openai.ChatCompletion.acreate(
                model="gpt-4-turbo",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=128,
                temperature=0.4,
            )
            return resp.choices[0].message.content.strip()
        except Exception as e:
            logger.warning(f"[TokenClassifier] LLM rationale error: {e}")
            return None

# ---- All Telegram audit/scan modules should import from here; ready for group, dashboard, or LLM explainer plug-ins ----
