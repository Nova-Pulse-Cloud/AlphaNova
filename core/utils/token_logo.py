# File: core/utils/token_logo.py
"""
Production-grade Token Logo Service for Nova Pulse.
Handles fetching, caching, resizing, and placeholder generation for token icons.
"""
import asyncio
import logging
from io import BytesIO
from pathlib import Path
from typing import Optional, List

import httpx
import redis.asyncio as redis_client
from PIL import Image, ImageDraw, ImageFont
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_logo")
logger.setLevel(logging.INFO)

# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────
LOGO_CACHE_HITS   = Counter("token_logo_cache_hits_total", "Cache hits for token logos")
LOGO_CACHE_MISSES = Counter("token_logo_cache_misses_total", "Cache misses for token logos")
LOGO_FETCH_ERRORS = Counter("token_logo_fetch_errors_total", "Errors fetching token logos")
SOURCE_SUCCESS    = Counter("token_logo_source_success_total", "Successful fetches per source", ["source"])
SOURCE_FAILURE    = Counter("token_logo_source_failure_total", "Failed fetches per source", ["source"])
LOGO_LATENCY      = Histogram("token_logo_latency_seconds", "Latency of token logo operations")

# ─────────────────────────────────────────────────────────────────────────────
# Configurable paths and cache
# ─────────────────────────────────────────────────────────────────────────────
TRUSTED_LOGO_SOURCES: List[str] = config.TRUSTED_LOGO_SOURCES
DEFAULT_LOGO_PATH = Path(config.DEFAULT_LOGO_PATH)
LOGO_DIR          = Path(config.LOGO_DIR)
LOGO_DIR.mkdir(parents=True, exist_ok=True)


class TokenLogoService:
    def __init__(self, ttl: int = 86400) -> None:
        self.ttl = ttl
        self.redis_url = config.REDIS_URL
        self._redis: Optional[redis_client.Redis] = None
        self._http = httpx.AsyncClient(timeout=5)

    async def _ensure_redis(self) -> None:
        if self._redis is None:
            try:
                self._redis = redis_client.from_url(self.redis_url, decode_responses=False)
                await self._redis.ping()
                logger.info("[TokenLogoService] Redis connected")
            except Exception as e:
                logger.error(f"[TokenLogoService] Redis connect failed: {e}")
                self._redis = None

    async def close(self) -> None:
        await self._http.aclose()
        if self._redis:
            await self._redis.close()

    async def get_logo(self, mint: str, size: int = 128) -> bytes:
        key = f"token_logo:{mint}:{size}"
        async with LOGO_LATENCY.time():
            await self._ensure_redis()
            data = await self._fetch_cache(key)
            if data:
                return data
            payload = await self._retrieve(mint, size)
            await self._store_cache(key, payload)
            await self._persist_to_disk(mint, size, payload)
            return payload

    async def _fetch_cache(self, key: str) -> Optional[bytes]:
        if not self._redis:
            LOGO_CACHE_MISSES.inc()
            return None
        try:
            result = await self._redis.get(key)
            if result:
                LOGO_CACHE_HITS.inc()
                return result
        except Exception as e:
            logger.warning(f"[TokenLogoService] Cache get error: {e}")
        LOGO_CACHE_MISSES.inc()
        return None

    async def _store_cache(self, key: str, data: bytes) -> None:
        if not self._redis:
            return
        try:
            await self._redis.set(key, data, ex=self.ttl)
        except Exception as e:
            logger.warning(f"[TokenLogoService] Cache set error: {e}")

    async def _retrieve(self, mint: str, size: int) -> bytes:
        for template in TRUSTED_LOGO_SOURCES:
            url = template.format(mint=mint)
            try:
                async for _ in AsyncRetrying(
                    stop=stop_after_attempt(3),
                    wait=wait_exponential_jitter(1, 3),
                    reraise=True,
                ):
                    with LOGO_LATENCY.time():
                        resp = await self._http.get(url)
                        resp.raise_for_status()
                data = resp.content
                resized = await asyncio.to_thread(self._resize, data, size)
                SOURCE_SUCCESS.labels(source=url).inc()
                return resized
            except Exception as e:
                SOURCE_FAILURE.labels(source=url).inc()
                logger.debug(f"[TokenLogoService] fetch failed {url}: {e}")
        LOGO_FETCH_ERRORS.inc()
        return await asyncio.to_thread(self._placeholder, mint, size)

    def _resize(self, raw: bytes, size: int) -> bytes:
        try:
            img = Image.open(BytesIO(raw)).convert("RGBA")
            img = img.resize((size, size), Image.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception as e:
            LOGO_FETCH_ERRORS.inc()
            logger.error(f"[TokenLogoService] resize failed: {e}")
            return raw

    def _placeholder(self, mint: str, size: int) -> bytes:
        try:
            if DEFAULT_LOGO_PATH.exists():
                raw = DEFAULT_LOGO_PATH.read_bytes()
                return self._resize(raw, size)
        except Exception as e:
            logger.warning(f"[TokenLogoService] default asset load failed from {DEFAULT_LOGO_PATH}: {e}")

        logger.warning(f"[TokenLogoService] generating dynamic placeholder for {mint}")
        img = Image.new("RGBA", (size, size), (40, 40, 40, 255))
        draw = ImageDraw.Draw(img)
        text = mint[:4].upper()
        font = ImageFont.load_default()
        w, h = draw.textsize(text, font=font)
        draw.text(((size - w) / 2, (size - h) / 2), text, font=font, fill=(255, 255, 255, 255))
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    async def _persist_to_disk(self, mint: str, size: int, raw: bytes) -> None:
        try:
            path = LOGO_DIR / f"{mint}_{size}.png"
            await asyncio.to_thread(path.write_bytes, raw)
        except Exception as e:
            logger.warning(f"[TokenLogoService] disk write failed: {e}")
