# File: utils/pnl_card.py

import io
import logging
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict

from core.alpha_memory_mesh import MemoryMesh

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths (consider moving to Config)
BACKGROUND_PATH = "/mnt/data/Nova1.png"
FONT_PATH       = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT_SIZE = 24
TEXT_FONT_SIZE  = 16

# Load fonts with fallback
try:
    TITLE_FONT = ImageFont.truetype(FONT_PATH, TITLE_FONT_SIZE)
    TEXT_FONT  = ImageFont.truetype(FONT_PATH, TEXT_FONT_SIZE)
except IOError:
    logger.warning("Font file not found, using default font.")
    TITLE_FONT = ImageFont.load_default()
    TEXT_FONT  = ImageFont.load_default()

# Cache background image
try:
    _BG = Image.open(BACKGROUND_PATH).convert("RGBA").resize((800, 400))
except Exception as e:
    logger.error(f"Failed to load background image: {e}")
    _BG = Image.new("RGBA", (800, 400), "black")

# Instantiate MemoryMesh for async data
_mesh = MemoryMesh()

async def generate_pnl_card(symbol: str) -> io.BytesIO:
    """
    Generate a PnL card for a given symbol, showing cycle status and win rate.
    """
    output = io.BytesIO()
    try:
        overlay = Image.new("RGBA", _BG.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # Fetch dynamic data
        win_rate = await _mesh.get_token_win_rate(symbol)
        cycle    = await _mesh.get_token_cycle_status(symbol)

        # Draw content
        draw.text((30,  20), f"Nova Pulse - {symbol} PnL Snapshot", font=TITLE_FONT, fill="white")
        draw.text((30, 100), f"Cycle: {cycle}",              font=TEXT_FONT, fill=(220,255,255))
        draw.text((30, 140), f"Win Rate: {win_rate*100:.2f}%", font=TEXT_FONT, fill=(180,255,180))
        draw.text((30, 180), f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC", font=TEXT_FONT, fill=(180,180,180))
        draw.text((30, 230), "@NovaPulseAI",                  font=TEXT_FONT, fill=(255,215,0))

        combined = Image.alpha_composite(_BG, overlay)
        combined.convert("RGB").save(output, format="PNG")
    except Exception as e:
        logger.error(f"Error generating PnL card for {symbol}: {e}")
        # Fallback red image
        fallback = Image.new("RGB", _BG.size, (255,0,0))
        fallback.save(output, format="PNG")
    finally:
        output.seek(0)
        return output
