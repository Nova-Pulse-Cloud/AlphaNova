# File: core/utils/signal_card.py

import io
import logging
import os
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Tuple, Any

from core.signal_generation.sl_manager import get_last_sl_tp_for_token
from core.utils.config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths (robust to deployment)
MASCOT_PATH = config.MASCOT_IMAGE_PATH or os.getenv("MASCOT_IMAGE_PATH", "assets/mascot/Nova1.png")
FONT_PATH   = config.FONT_PATH or os.getenv("FONT_PATH", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")
TITLE_FONT_SIZE = 32
TEXT_FONT_SIZE  = 18
WATERMARK_FONT_SIZE = 21

# Load fonts with robust fallback

def load_font(font_path: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(font_path, size)
    except Exception:
        logger.warning("Font file not found at %s, using default font.", font_path)
        return ImageFont.load_default()

TITLE_FONT = load_font(FONT_PATH, TITLE_FONT_SIZE)
TEXT_FONT  = load_font(FONT_PATH, TEXT_FONT_SIZE)
WATERMARK_FONT = load_font(FONT_PATH, WATERMARK_FONT_SIZE)

async def generate_signal_card(signal: Dict[str, Any], mascot_path: str = None) -> io.BytesIO:
    """
    Nova Pulse - Elite Signal Card Generator with branding, watermark, mascot, SL/TP, ready for Telegram.
    """
    # ---- Mascot image loading ----
    img_path = mascot_path or MASCOT_PATH
    if not os.path.isfile(img_path):
        logger.error("Mascot image not found at %s", img_path)
        raise FileNotFoundError(f"Mascot image not found: {img_path}")
    bg = Image.open(img_path).convert("RGBA").resize((900, 480))
    overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    # ---- Extract signal fields ----
    symbol    = signal.get("symbol", "N/A")
    score     = float(signal.get("score", 0.0))
    entry     = float(signal.get("entry", 0.0))
    sl        = signal.get("sl")
    tp        = signal.get("tp")
    breakdown = signal.get("breakdown", [])
    flavor    = signal.get("flavor", "Moderate")
    risk      = signal.get("risk", "?")
    meta      = signal.get("meta_features", {})

    # ---- Fallback SL/TP ----
    if sl is None or tp is None:
        fallback = await get_last_sl_tp_for_token(symbol)
        sl = sl or fallback.get("sl")
        tp = tp or fallback.get("tp")

    # ---- Header and Key Metrics ----
    draw.text((30, 30), f"Nova Pulse Signal: {symbol}", font=TITLE_FONT, fill="white")
    draw.text((30, 80), f"Confidence: {score:.2f}", font=TEXT_FONT, fill=(200,255,200))
    draw.text((30,110), f"Entry: {entry:.6f}", font=TEXT_FONT, fill="white")
    draw.text((30,140), f"TP: {float(tp):.6f}" if tp else "TP: ?", font=TEXT_FONT, fill=(120,255,120))
    draw.text((30,170), f"SL: {float(sl):.6f}" if sl else "SL: ?", font=TEXT_FONT, fill=(255,120,120))
    draw.text((30,200), f"Flavor: {flavor} | Risk: {risk}", font=TEXT_FONT, fill=(255,200,100))

    # ---- Agent breakdown and meta ----
    sources = ", ".join(item.get("source", "?") for item in breakdown) or "Unknown"
    draw.text((30,240), f"Sources: {sources}", font=TEXT_FONT, fill=(180,200,255))
    if meta:
        draw.text((30,270), f"Meta: {meta}", font=TEXT_FONT, fill=(180,210,220))

    # ---- Timestamp and Brand ----
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    draw.text((30,350), f"Generated: {ts} UTC", font=TEXT_FONT, fill=(140,140,140))

    # ---- Watermark (bottom right) ----
    watermark = "@NovaPulseAI"
    w, h = draw.textsize(watermark, font=WATERMARK_FONT)
    draw.text((bg.width - w - 30, bg.height - h - 18), watermark, font=WATERMARK_FONT, fill=(0,255,255,128))

    # ---- Compose and return PNG bytes ----
    final = Image.alpha_composite(bg, overlay).convert("RGB")
    output = io.BytesIO()
    final.save(output, format="PNG", optimize=True)
    output.seek(0)
    return output
