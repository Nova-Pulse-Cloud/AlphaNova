# File: utils/signal_card.py

import io
import logging
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict

from core.signal_generation.sl_manager import get_last_sl_tp_for_token

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths (consider making configurable via Config)
BACKGROUND_PATH = "/mnt/data/Nova1.png"
FONT_PATH       = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT_SIZE = 26
TEXT_FONT_SIZE  = 16

# Load fonts, fallback to default if unavailable
try:
    TITLE_FONT = ImageFont.truetype(FONT_PATH, TITLE_FONT_SIZE)
    TEXT_FONT  = ImageFont.truetype(FONT_PATH, TEXT_FONT_SIZE)
except IOError:
    logger.warning("Font file not found, using default font.")
    TITLE_FONT = ImageFont.load_default()
    TEXT_FONT  = ImageFont.load_default()

async def generate_signal_card(signal: Dict) -> io.BytesIO:
    """
    Nova Pulse - Signal Card Renderer with enhanced error handling, branding, and SL/TP fallback.
    Generates an image representing the trading signal, including entry, stop loss, take profit, and agent breakdown.
    """
    try:
        # Load and prepare background
        bg = Image.open(BACKGROUND_PATH).convert("RGBA").resize((800, 400))
        overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # Extract signal details
        symbol    = signal.get("symbol", "N/A")
        score     = float(signal.get("score", 0.0))
        entry     = float(signal.get("entry", 0.0))
        sl        = signal.get("sl")
        tp        = signal.get("tp")
        breakdown = signal.get("breakdown", [])

        # Fallback for missing SL/TP
        if sl is None or tp is None:
            fallback = await get_last_sl_tp_for_token(symbol)
            sl = sl or fallback.get("sl")
            tp = tp or fallback.get("tp")

        # Draw header and key metrics
        draw.text((30,  20), f"Nova Pulse Signal: {symbol}",     font=TITLE_FONT, fill="white")
        draw.text((30,  80), f"Confidence Score: {score:.2f}",     font=TEXT_FONT,  fill=(200,255,200))
        draw.text((30, 110), f"Entry Price: {entry:.4f}",         font=TEXT_FONT,  fill="white")

        # Draw SL/TP
        if sl is not None:
            draw.text((30, 140), f"Stop Loss: {float(sl):.4f}",   font=TEXT_FONT,  fill=(255,120,120))
        if tp is not None:
            draw.text((30, 170), f"Take Profit: {float(tp):.4f}", font=TEXT_FONT,  fill=(120,255,120))

        # Agent breakdown
        sources = ", ".join(item.get("source", "?") for item in breakdown) or "Unknown"
        draw.text((30, 220), f"Fused From: {sources}",           font=TEXT_FONT,  fill=(180,200,255))

        # Timestamp and branding
        draw.text((30, 260), f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC", font=TEXT_FONT, fill=(140,140,140))
        draw.text((30, 310), "@NovaPulseAI",                     font=TEXT_FONT, fill=(255,215,0))

        # Composite and output
        combined = Image.alpha_composite(bg, overlay)
        output = io.BytesIO()
        combined.convert("RGB").save(output, format="PNG")
        output.seek(0)
        return output

    except Exception as e:
        logger.error(f"Failed to generate signal card for {signal.get('symbol')}: {e}")
        raise
