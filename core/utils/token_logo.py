# core/utils/token_logo.py

import asyncio
import logging
from pathlib import Path
from io import BytesIO
from typing import Optional, List

import httpx
import redis.asyncio as redis_client
from PIL import Image, ImageDraw, ImageFont
from tenacity import AsyncRetrying, stop_after_attempt, wait_exponential_jitter
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_logo")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
LOGO_CACHE_HITS    = Counter("token_logo_cache_hits_total",   "Cache hits for token logos")
LOGO_CACHE_MISSES  = Counter("token_logo_cache_misses_total", "Cache misses for token logos")
LOGO_FETCH_ERRORS  = Counter("token_logo_fetch_errors_total","Errors fetching token logos")
SOURCE_SUCCESS     = Counter("token_logo_source_success_total","Successful fetches per source", ["source"])
SOURCE_FAILURE     = Counter("token_logo_source_failure_total","Failed fetches per source", ["source"])
LOGO_LATENCY       = Histogram("token_logo_latency_seconds",   "Latency of token logo operations")

# ────────────────────────────────────────────────────────────────────────────────
# Configurable sources and assets
# ────────────────────────────────────────────────────────────────────────────────
TRUSTED_LOGO_SOURCES: List[str] = config.TRUSTED_LOGO_SOURCES  # List of URL templates
DEFAULT_LOGO_PATH = Path(config.DEFAULT_LOGO_PATH)
LOGO_DIR          = Path(config.LOGO_DIR)
LOGO_DIR.mkdir(parents=True, exist_ok=True)

class TokenLogoService:
    """
    Production-grade TokenLogoService:
    - Redis caching with stampede lock
    - Async fetch with retry/backoff
    - Offload heavy PIL ops to thread pool
    - Per-source success/failure metrics
    - Configurable placeholder and disk persistence
    """
    def __init__(self, ttl: int = 86400) -> None:
        self.redis_url = config.REDIS_URL
        self.ttl       = ttl
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=5)

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=False)
            await self.redis.ping()
            logger.info("[TokenLogoService] Redis connected")
        except Exception as e:
            logger.error(f"[TokenLogoService] Redis connect failed: {e}")
            self.redis = None

    async def disconnect(self) -> None:
        await self.http.aclose()
        if self.redis:
            await self.redis.close()

    async def get_logo(self, mint: str, size: int = 128) -> bytes:
        cache_key = f"token_logo:{mint}:{size}"
        async with LOGO_LATENCY.time():
            await self.connect()
            # Stampede lock
            lock = None
            if self.redis:
                lock = self.redis.lock(f"{cache_key}:lock", timeout=10)
                have = await lock.acquire(blocking=False)
                # If another fetch in progress, wait for it
                if not have:
                    try:
                        data = await self.redis.get(cache_key)
                        if data:
                            LOGO_CACHE_HITS.inc()
                            return data
                    except: pass
            # Check cache
            if self.redis:
                try:
                    data = await self.redis.get(cache_key)
                    if data:
                        LOGO_CACHE_HITS.inc()
                        if lock:
                            await lock.release()
                        return data
                except Exception as e:
                    logger.warning(f"[TokenLogoService] Cache get error: {e}")
            LOGO_CACHE_MISSES.inc()

            # Fetch or generate
            img_bytes = await self._fetch_logo_online(mint, size)
            if not img_bytes:
                img_bytes = await self._generate_placeholder(mint, size)

            # Cache
            if self.redis:
                try:
                    await self.redis.set(cache_key, img_bytes, ex=self.ttl)
                except Exception as e:
                    logger.warning(f"[TokenLogoService] Cache set error: {e}")
                if lock:
                    await lock.release()

            # Persist to disk
            out_path = LOGO_DIR / f"{mint}_{size}.png"
            try:
                out_path.write_bytes(img_bytes)
            except Exception as e:
                logger.warning(f"[TokenLogoService] Disk write failed: {e}")

            return img_bytes

    async def _fetch_logo_online(self, mint: str, size: int) -> Optional[bytes]:
        for tpl in TRUSTED_LOGO_SOURCES:
            url = tpl.format(mint=mint)
            try:
                for attempt in AsyncRetrying(stop=stop_after_attempt(3), wait=wait_exponential_jitter(1, 3), reraise=True):
                    with LOGO_LATENCY.time():
                        resp = await self.http.get(url)
                        resp.raise_for_status()
                data = resp.content
                resized = await asyncio.to_thread(self._resize, data, size)
                SOURCE_SUCCESS.labels(source=url).inc()
                return resized
            except Exception as e:
                SOURCE_FAILURE.labels(source=url).inc()
                logger.debug(f"[TokenLogoService] Fetch failed {url}: {e}")
        LOGO_FETCH_ERRORS.inc()
        return None

    def _resize(self, data: bytes, size: int) -> bytes:
        try:
            img = Image.open(BytesIO(data)).convert("RGBA")
            img = img.resize((size, size), Image.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        except Exception as e:
            LOGO_FETCH_ERRORS.inc()
            logger.error(f"[TokenLogoService] Resize failed: {e}")
            return data

    async def _generate_placeholder(self, mint: str, size: int) -> bytes:
        # Default asset
        if DEFAULT_LOGO_PATH.exists():
            try:
                data = DEFAULT_LOGO_PATH.read_bytes()
                return await asyncio.to_thread(self._resize, data, size)
            except Exception as e:
                logger.warning(f"[TokenLogoService] Default asset load failed: {e}")
        # Dynamic placeholder
        def make():
            img = Image.new("RGBA", (size, size), (50, 50, 50, 255))
            draw = ImageDraw.Draw(img)
            font = ImageFont.load_default()
            text = mint[:4].upper()
            w, h = draw.textsize(text, font=font)
            draw.text(((size-w)/2, (size-h)/2), text, font=font, fill=(255,255,255,255))
            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()
        return await asyncio.to_thread(make)
