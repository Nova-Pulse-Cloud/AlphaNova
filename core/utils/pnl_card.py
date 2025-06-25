# core/utils/pnl_card.py

import io
import logging
import asyncio
import time
from pathlib import Path
from typing import Any, Dict, Optional

from PIL import Image, ImageDraw, ImageFont
from prometheus_client import Counter, Histogram

from core.utils.config import config
from core.utils.trade_memory import TradeMemory

logger = logging.getLogger("nova.pnl_card")
logger.setLevel(logging.INFO)

# Prometheus metrics
PNL_CARD_GENERATED = Counter(
    "pnl_card_generated_total", "Total PnL cards generated"
)
PNL_CARD_ERRORS = Counter(
    "pnl_card_errors_total", "Errors generating PnL cards"
)
PNL_CARD_LATENCY = Histogram(
    "pnl_card_latency_seconds", "Latency of PnL card generation"
)

# Asset and font paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
LOGO_PATH = BASE_DIR / "assets" / "mascot" / "Nova1.png"
FONT_PATH = BASE_DIR / "assets" / "fonts" / "DejaVuSans-Bold.ttf"

# Card dimensions
WIDTH, HEIGHT = 900, 440


def _load_font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(str(path), size)
    except Exception:
        logger.warning("[PnLCard] Font not found at %s, using default.", path)
        return ImageFont.load_default()

TITLE_FONT = _load_font(FONT_PATH, 28)
TEXT_FONT = _load_font(FONT_PATH, 18)
WATERMARK_FONT = _load_font(FONT_PATH, 16)

# Load background image or fallback to solid color
try:
    _BG = (
        Image.open(LOGO_PATH)
        .convert("RGBA")
        .resize((WIDTH, HEIGHT))
    )
except Exception as e:
    logger.error("[PnLCard] Failed to load background from %s: %s", LOGO_PATH, e)
    _BG = Image.new("RGBA", (WIDTH, HEIGHT), (0, 0, 0, 255))

# Trade memory for snapshot cards
_trade_memory = TradeMemory(getattr(config, "TRADE_MEMORY_DB_PATH", None))

async def generate_pnl_card(
    symbol: Optional[str] = None,
    trade: Optional[Dict[str, Any]] = None,
    user: Optional[str] = None,
) -> io.BytesIO:
    """
    Generate a PnL card as PNG bytes.
    If `trade` is provided, render a detailed closed-trade breakdown.
    Otherwise, render a PnL snapshot for the given symbol.
    """
    buffer = io.BytesIO()
    start_time = time.time()
    PNL_CARD_GENERATED.inc()

    try:
        # Prepare overlay drawing
        overlay = Image.new("RGBA", _BG.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())

        if trade:
            # Detailed trade card
            sym = trade.get("symbol", "N/A")
            pnl = float(trade.get("pnl", 0.0))
            entry = float(trade.get("entry", 0.0))
            exit_price = float(trade.get("exit", 0.0))
            side = trade.get("side", "").upper()
            reason = trade.get("reason", "Closed")
            usr = trade.get("user", user or "N/A")

            draw.text((40, 30), f"Trade Closed: {sym}", font=TITLE_FONT, fill=(255, 255, 255))
            draw.text((40, 80), f"Side: {side}", font=TEXT_FONT, fill=(135, 206, 250))
            draw.text((40, 120), f"Entry: {entry:.6f}", font=TEXT_FONT, fill=(255, 255, 255))
            draw.text((40, 160), f"Exit: {exit_price:.6f}", font=TEXT_FONT, fill=(255, 255, 255))
            color = (144, 238, 144) if pnl >= 0 else (255, 127, 127)
            draw.text((40, 200), f"PnL: {pnl:.6f}", font=TITLE_FONT, fill=color)
            draw.text((40, 250), f"Reason: {reason}", font=TEXT_FONT, fill=(255, 215, 0))
            draw.text((40, 290), f"User: {usr}", font=TEXT_FONT, fill=(176, 196, 222))
            draw.text((40, 330), f"Closed: {timestamp}", font=TEXT_FONT, fill=(169, 169, 169))
        else:
            # Snapshot card
            sym = symbol or "N/A"
            loop = asyncio.get_running_loop()
            history = await loop.run_in_executor(None, _trade_memory.get_symbol_history, sym)
            win_rate = _trade_memory.win_rate(sym)
            total_pnl = _trade_memory.total_pnl(sym)
            count = len(history)

            draw.text((40, 30), f"Nova Pulse – {sym} PnL Snapshot", font=TITLE_FONT, fill=(255, 255, 255))
            draw.text((40, 100), f"Trades: {count}", font=TEXT_FONT, fill=(173, 216, 230))
            draw.text((40, 140), f"Win Rate: {win_rate * 100:.2f}%", font=TEXT_FONT, fill=(144, 238, 144))
            draw.text((40, 180), f"Total PnL: {total_pnl:.6f}", font=TEXT_FONT, fill=(255, 250, 205))
            draw.text((40, 220), f"Generated: {timestamp}", font=TEXT_FONT, fill=(192, 192, 192))

        # Watermark
        watermark = "@NovaPulseAI"
        w, h = draw.textsize(watermark, font=WATERMARK_FONT)
        draw.text(
            (WIDTH - w - 20, HEIGHT - h - 10),
            watermark,
            font=WATERMARK_FONT,
            fill=(0, 255, 255, 180),
        )

        # Composite and save
        card = Image.alpha_composite(_BG, overlay).convert("RGB")
        card.save(buffer, format="PNG")
    except Exception as e:
        PNL_CARD_ERRORS.inc()
        logger.error("[PnLCard] Error generating card: %s", e)
        # Fallback red image
        fallback = Image.new("RGB", _BG.size, (255, 0, 0))
        fallback.save(buffer, format="PNG")
    finally:
        PNL_CARD_LATENCY.observe(time.time() - start_time)
        buffer.seek(0)
        return buffer
