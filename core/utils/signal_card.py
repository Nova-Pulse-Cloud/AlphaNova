# File: utils/signal_card.py

import io
import logging
import os
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict

from core.signal_generation.sl_manager import get_last_sl_tp_for_token
from utils.config import Config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

config = Config()

# Paths (robust to deployment)
MASCOT_PATH = getattr(config, "MASCOT_IMAGE_PATH", None) or os.getenv("MASCOT_IMAGE_PATH", "assets/mascot/Nova1.png")
FONT_PATH   = getattr(config, "FONT_PATH", None) or "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT_SIZE = 32
TEXT_FONT_SIZE  = 18
WATERMARK_FONT_SIZE = 21

# Load fonts with robust fallback
def load_font(font_path, size):
    try:
        return ImageFont.truetype(font_path, size)
    except Exception:
        logger.warning("Font file not found, using default font.")
        return ImageFont.load_default()

TITLE_FONT = load_font(FONT_PATH, TITLE_FONT_SIZE)
TEXT_FONT  = load_font(FONT_PATH, TEXT_FONT_SIZE)
WATERMARK_FONT = load_font(FONT_PATH, WATERMARK_FONT_SIZE)

async def generate_signal_card(signal: Dict, mascot_path: str = None) -> io.BytesIO:
    """
    Nova Pulse - Elite Signal Card Generator with branding, watermark, mascot, SL/TP, and ready for Telegram sharing.
    """
    try:
        # ---- Mascot image loading ----
        mascot_img_path = mascot_path or MASCOT_PATH
        if not os.path.isfile(mascot_img_path):
            logger.error(f"Mascot image not found at {mascot_img_path}")
            raise FileNotFoundError(f"Mascot image not found at {mascot_img_path}")
        bg = Image.open(mascot_img_path).convert("RGBA").resize((900, 480))
        overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # ---- Signal extraction ----
        symbol    = signal.get("symbol", "N/A")
        score     = float(signal.get("score", 0.0))
        entry     = float(signal.get("entry", 0.0))
        sl        = signal.get("sl")
        tp        = signal.get("tp")
        breakdown = signal.get("breakdown", [])
        flavor    = signal.get("flavor", "")
        risk      = signal.get("risk", "")
        meta      = signal.get("meta_features", {})
        user      = signal.get("user", "")

        # ---- Fallback for missing SL/TP ----
        if sl is None or tp is None:
            fallback = await get_last_sl_tp_for_token(symbol)
            sl = sl or fallback.get("sl")
            tp = tp or fallback.get("tp")

        # ---- Header and Key Metrics ----
        draw.text((30,  30), f"Nova Pulse Signal: {symbol}", font=TITLE_FONT, fill="white")
        draw.text((30,  80), f"Confidence: {score:.2f}", font=TEXT_FONT, fill=(200,255,200))
        draw.text((30, 110), f"Entry: {entry:.6f}", font=TEXT_FONT, fill="white")
        draw.text((30, 140), f"TP: {float(tp):.6f}" if tp else "TP: ?", font=TEXT_FONT, fill=(120,255,120))
        draw.text((30, 170), f"SL: {float(sl):.6f}" if sl else "SL: ?", font=TEXT_FONT, fill=(255,120,120))
        draw.text((30, 200), f"Flavor: {flavor or 'Moderate'} | Risk: {risk if risk != '' else '?'}", font=TEXT_FONT, fill=(255, 200, 100))
        
        # ---- Agent breakdown and meta ----
        sources = ", ".join(item.get("source", "?") for item in breakdown) or "Unknown"
        draw.text((30, 240), f"Sources: {sources}", font=TEXT_FONT, fill=(180,200,255))
        if meta:
            draw.text((30, 270), f"Meta: {str(meta)}", font=TEXT_FONT, fill=(180,210,220))
        if user:
            draw.text((30, 300), f"User: {user}", font=TEXT_FONT, fill=(160,160,180))

        # ---- Timestamp and Brand ----
        draw.text((30, 350), f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC", font=TEXT_FONT, fill=(140,140,140))
        
        # ---- Watermark (bottom right, semi-transparent) ----
        watermark = "@NovaPulseAI"
        w, h = draw.textsize(watermark, font=WATERMARK_FONT)
        draw.text((bg.width - w - 30, bg.height - h - 18), watermark, font=WATERMARK_FONT, fill=(0,255,255,128))

        # ---- Final Compose and Output ----
        final_img = Image.alpha_composite(bg, overlay)
        output = io.BytesIO()
        final_img.convert("RGB").save(output, format="PNG", optimize=True)
        output.seek(0)
        return output

    except Exception as e:
        logger.error(f"Failed to generate signal card for {signal.get('symbol')}: {e}")
        raise
