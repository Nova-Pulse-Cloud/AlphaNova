import os
import logging
from pathlib import Path
from io import BytesIO

import aiohttp
from redis import asyncio as aioredis
from PIL import Image, ImageDraw, ImageFont

logger = logging.getLogger("token_logo")

LOGO_DIR = Path("data/logos")
LOGO_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_LOGO_PATH = "assets/placeholder.png"

TRUSTED_LOGO_SOURCES = [
    "https://raw.githubusercontent.com/trustwallet/assets/master/blockchains/solana/assets/{mint}/logo.png",
    "https://cdn.jsdelivr.net/gh/solana-labs/token-list/assets/mainnet/{mint}/logo.png"
]

class TokenLogoService:
    def __init__(self, redis_url: str, logo_dir: Path = LOGO_DIR, ttl: int = 86400):
        """
        Initializes the TokenLogoService with Redis connection, logo directory, and TTL settings.
        
        :param redis_url: Redis URL for cache storage.
        :param logo_dir: Directory to store logo files.
        :param ttl: Cache expiration time (default: 86400 seconds).
        """
        self.redis_url = redis_url
        self.logo_dir = logo_dir
        self.redis = None
        self.ttl = ttl

    async def connect(self):
        """Connects to Redis."""
        if not self.redis:
            self.redis = await aioredis.from_url(self.redis_url, encoding="utf8", decode_responses=False)
            logger.info("✅ TokenLogoService connected to Redis")

    async def get_logo(self, mint: str, size: int = 128) -> bytes:
        """
        Retrieves the logo for the token. It first checks the Redis cache, then fetches from online sources if necessary.
        
        :param mint: The mint address of the token.
        :param size: The desired logo size (default: 128).
        :return: A byte string containing the logo image.
        """
        await self.connect()
        cache_key = f"token_logo:{mint}:{size}"
        cached = await self.redis.get(cache_key)
        if cached:
            logger.info(f"✔️ Cache hit for logo: {mint}")
            return cached

        image_bytes = await self._fetch_logo_online(mint, size)
        if not image_bytes:
            logger.info(f"🔄 Fallback: Generating placeholder for logo: {mint}")
            image_bytes = await self._get_or_generate_placeholder(mint, size)

        await self.redis.set(cache_key, image_bytes, ex=self.ttl)
        return image_bytes

    async def _fetch_logo_online(self, mint: str, size: int) -> bytes:
        """
        Attempts to fetch the token logo from trusted online sources.
        
        :param mint: The mint address of the token.
        :param size: The desired logo size.
        :return: A byte string containing the resized logo, or None if not found.
        """
        for url_template in TRUSTED_LOGO_SOURCES:
            url = url_template.format(mint=mint)
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=4) as response:
                        if response.status == 200:
                            logger.info(f"✔️ Logo found at {url}")
                            data = await response.read()
                            resized = self._resize_logo(data, size)
                            return resized
                        else:
                            logger.warning(f"⚠️ Logo not found at {url}")
            except Exception as e:
                logger.warning(f"⚠️ Logo fetch failed from {url}: {e}")
        return None

    def _resize_logo(self, img_bytes: bytes, size: int) -> bytes:
        """
        Resizes the logo to the specified size.

        :param img_bytes: The raw image bytes.
        :param size: The desired logo size.
        :return: Resized logo as bytes.
        """
        with Image.open(BytesIO(img_bytes)) as img:
            img = img.convert("RGBA")
            img = img.resize((size, size), Image.LANCZOS)
            buf = BytesIO()
            img.save(buf, format="PNG")
            return buf.getvalue()

    async def _get_or_generate_placeholder(self, mint: str, size: int = 128) -> bytes:
        """
        Fetches the placeholder logo if no logo is found or if the fetch fails.
        
        :param mint: The mint address of the token (for placeholder text).
        :param size: The desired logo size.
        :return: The generated placeholder logo as bytes.
        """
        try:
            if Path(DEFAULT_LOGO_PATH).exists():
                with open(DEFAULT_LOGO_PATH, "rb") as f:
                    return self._resize_logo(f.read(), size)
        except Exception:
            logger.warning("⚠️ Placeholder logo not found, generating synthetic.")

        # Generate fallback image dynamically
        img = Image.new("RGB", (size, size), (50, 50, 50))
        draw = ImageDraw.Draw(img)
        font = ImageFont.load_default()
        text = mint[:4].upper()
        draw.text((size//4, size//3), text, fill=(255, 255, 255), font=font)
        buf = BytesIO()
        img.save(buf, format="PNG")
        return buf.getvalue()
