# File: utils/pnl_card.py

import io
import logging
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from typing import Dict, Optional

from core.alpha_memory_mesh import MemoryMesh

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths (consider moving to Config/env)
BACKGROUND_PATH = "/mnt/data/Nova1.png"
FONT_PATH       = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT_SIZE = 28
TEXT_FONT_SIZE  = 17
WATERMARK_FONT_SIZE = 19

# Load fonts with fallback
def load_font(font_path, size):
    try:
        return ImageFont.truetype(font_path, size)
    except IOError:
        logger.warning("Font file not found, using default font.")
        return ImageFont.load_default()

TITLE_FONT = load_font(FONT_PATH, TITLE_FONT_SIZE)
TEXT_FONT  = load_font(FONT_PATH, TEXT_FONT_SIZE)
WATERMARK_FONT = load_font(FONT_PATH, WATERMARK_FONT_SIZE)

# Load mascot background
try:
    _BG = Image.open(BACKGROUND_PATH).convert("RGBA").resize((900, 440))
except Exception as e:
    logger.error(f"Failed to load mascot image: {e}")
    _BG = Image.new("RGBA", (900, 440), "black")

# Instantiate MemoryMesh for async data
_mesh = MemoryMesh()

async def generate_pnl_card(
    symbol: Optional[str] = None,
    trade: Optional[Dict] = None,
    user: Optional[str] = None
) -> io.BytesIO:
    """
    Generate a PnL card:
    - If trade dict provided, show full trade breakdown (closed trade, PnL, entry/exit).
    - If symbol provided, show PnL snapshot (cycle, win rate).
    """
    output = io.BytesIO()
    try:
        overlay = Image.new("RGBA", _BG.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        if trade:
            # Closed trade card with all details
            symbol = trade.get("symbol", "N/A")
            pnl    = float(trade.get("pnl", 0.0))
            entry  = float(trade.get("entry", 0.0))
            exit_  = float(trade.get("exit", 0.0))
            side   = trade.get("side", "N/A").upper()
            reason = trade.get("reason", "closed")
            user_  = trade.get("user", user or "N/A")

            # Main details
            draw.text((40,  30), f"Trade Closed: {symbol}",     font=TITLE_FONT, fill="white")
            draw.text((40,  80), f"Side: {side}",                font=TEXT_FONT, fill=(120,200,255))
            draw.text((40, 120), f"Entry: {entry:.6f}",          font=TEXT_FONT, fill="white")
            draw.text((40, 150), f"Exit: {exit_:.6f}",           font=TEXT_FONT, fill="white")
            draw.text((40, 190), f"PnL: {pnl:.6f}",              font=TITLE_FONT, fill=(120,255,120) if pnl > 0 else (255,120,120))
            draw.text((40, 230), f"Reason: {reason}",            font=TEXT_FONT, fill=(255,210,128))
            draw.text((40, 260), f"User: {user_}",               font=TEXT_FONT, fill=(200,200,240))
            draw.text((40, 300), f"Closed: {timestamp} UTC",     font=TEXT_FONT, fill=(140,140,140))
        else:
            # Generic symbol PnL snapshot (memory/cycle)
            if not symbol:
                symbol = "N/A"
            win_rate = await _mesh.get_token_win_rate(symbol)
            cycle    = await _mesh.get_token_cycle_status(symbol)
            draw.text((40,  30), f"Nova Pulse - {symbol} PnL Snapshot", font=TITLE_FONT, fill="white")
            draw.text((40, 100), f"Cycle: {cycle}",              font=TEXT_FONT, fill=(220,255,255))
            draw.text((40, 140), f"Win Rate: {win_rate*100:.2f}%", font=TEXT_FONT, fill=(180,255,180))
            draw.text((40, 180), f"Generated: {timestamp} UTC",  font=TEXT_FONT, fill=(180,180,180))

        # Watermark, mascot handle
        watermark = "@NovaPulseAI"
        w, h = draw.textsize(watermark, font=WATERMARK_FONT)
        draw.text((_BG.width - w - 30, _BG.height - h - 18), watermark, font=WATERMARK_FONT, fill=(0,255,255,190))

        # Compose & return
        combined = Image.alpha_composite(_BG, overlay)
        combined.convert("RGB").save(output, format="PNG")
    except Exception as e:
        logger.error(f"Error generating PnL card: {e}")
        fallback = Image.new("RGB", _BG.size, (255,0,0))
        fallback.save(output, format="PNG")
    finally:
        output.seek(0)
        return output
