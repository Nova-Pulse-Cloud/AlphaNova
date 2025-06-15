# core/utils/token_logo.py

import logging
import os
from pathlib import Path
from io import BytesIO
from typing import Optional

import httpx
import redis.asyncio as redis_client
from PIL import Image, ImageDraw, ImageFont
from prometheus_client import Counter, Histogram

from core.utils.config import config

logger = logging.getLogger("nova.token_logo")
logger.setLevel(logging.INFO)

# ────────────────────────────────────────────────────────────────────────────────
# Metrics
# ────────────────────────────────────────────────────────────────────────────────
LOGO_CACHE_HITS = Counter("token_logo_cache_hits_total", "Cache hits for token logos")
LOGO_CACHE_MISSES = Counter("token_logo_cache_misses_total", "Cache misses for token logos")
LOGO_FETCH_ERRORS = Counter("token_logo_fetch_errors_total", "Errors fetching token logos")
LOGO_LATENCY = Histogram("token_logo_latency_seconds", "Latency of token logo operations")

# ────────────────────────────────────────────────────────────────────────────────
# Trusted sources
# ────────────────────────────────────────────────────────────────────────────────
TRUSTED_LOGO_SOURCES = [
    "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/solana/assets/{mint}/logo.png",
    "https://cdn.jsdelivr.net/gh/solana-labs/token-list/assets/mainnet/{mint}/logo.png",
]

# Default placeholder asset
DEFAULT_LOGO_PATH = Path("assets/mascot/Nova1.png")
# Directory to persist fetched or generated logos
LOGO_DIR = Path("data/logos")
LOGO_DIR.mkdir(parents=True, exist_ok=True)

class TokenLogoService:
    """
    Fetches, resizes, caches, and serves token logos via Redis and disk.
    """
    def __init__(self, ttl: int = 86400, size: int = 128) -> None:
        self.redis_url = config.REDIS_URL
        self.ttl = ttl
        self.size = size
        self.redis: Optional[redis_client.Redis] = None
        self.http = httpx.AsyncClient(timeout=5)

    async def connect(self) -> None:
        try:
            self.redis = redis_client.from_url(self.redis_url, decode_responses=False)
            await self.redis.ping()
            logger.info("[TokenLogoService] Connected to Redis")
        except Exception as e:
            logger.error(f"[TokenLogoService] Redis connection failed: {e}")
            self.redis = None

    async def get_logo(self, mint: str, size: Optional[int] = None) -> bytes:
        size = size or self.size
        cache_key = f"token_logo:{mint}:{size}"
        with LOGO_LATENCY.time():
            # Check cache
            if self.redis:
                try:
                    data = await self.redis.get(cache_key)
                    if data:
                        LOGO_CACHE_HITS.inc()
                        return data
                except Exception as e:
                    logger.warning(f"[TokenLogoService] Redis get error: {e}")
            LOGO_CACHE_MISSES.inc()

            # Try fetch online
            img_bytes = await self._fetch_logo_online(mint, size)
            if not img_bytes:
                img_bytes = await self._generate_placeholder(mint, size)

            # Cache in Redis
            if self.redis:
                try:
                    await self.redis.set(cache_key, img_bytes, ex=self.ttl)
                except Exception as e:
                    logger.warning(f"[TokenLogoService] Redis set error: {e}")

            # Persist to disk
            out_path = LOGO_DIR / f"{mint}_{size}.png"
            try:
                with open(out_path, "wb") as f:
                    f.write(img_bytes)
            except Exception as e:
                logger.warning(f"[TokenLogoService] Disk write failed: {e}")

            return img_bytes

    async def _fetch_logo_online(self, mint: str, size: int) -> Optional[bytes]:
        for url_tpl in TRUSTED_LOGO_SOURCES:
            url = url_tpl.format(mint=mint)
            try:
                resp = await self.http.get(url)
                resp.raise_for_status()
                return self._resize(resp.content, size)
            except Exception as e:
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
        # Attempt default asset
        try:
            if DEFAULT_LOGO_PATH.exists():
                with open(DEFAULT_LOGO_PATH, "rb") as f:
                    return self._resize(f.read(), size)
        except Exception as e:
            logger.warning(f"[TokenLogoService] Default asset load failed: {e}")

        # Fallback dynamic placeholder
        img = Image.new("RGBA", (size, size), (30, 30, 30, 255))
        draw = ImageDraw.Draw(img)
        font = ImageFont.load_default()
        text = mint[:4].upper()
        w, h = draw.textsize(text, font=font)
        draw.text(((size-w)/2, (size-h)/2), text, font=font, fill=(255,255,255,255))
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()

    async def close(self) -> None:
        await self.http.aclose()
        if self.redis:
            await self.redis.close()
