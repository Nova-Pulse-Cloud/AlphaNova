import io
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional

from PIL import Image, ImageDraw, ImageFont

from core.signal_generation.sl_manager import get_last_sl_tp_for_token
from core.utils.config import config

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Constants with fallback (do not rely on config.FOO if not defined)
MASCOT_PATH = os.getenv("MASCOT_IMAGE_PATH", "assets/mascot/Nova1.png")
FONT_PATH = os.getenv("FONT_PATH", "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf")

CARD_WIDTH = 900
CARD_HEIGHT = 480
TITLE_FONT_SIZE = 32
TEXT_FONT_SIZE = 18
WATERMARK_FONT_SIZE = 21


def _load_font(path: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(path, size)
    except Exception:
        logger.warning("Font not found at %s, using default.", path)
        return ImageFont.load_default()

TITLE_FONT = _load_font(FONT_PATH, TITLE_FONT_SIZE)
TEXT_FONT = _load_font(FONT_PATH, TEXT_FONT_SIZE)
WATERMARK_FONT = _load_font(FONT_PATH, WATERMARK_FONT_SIZE)


async def generate_signal_card(
    signal: Dict[str, Any],
    mascot_path: Optional[str] = None
) -> io.BytesIO:
    path = mascot_path or MASCOT_PATH
    if not os.path.isfile(path):
        logger.error("Mascot image not found at %s", path)
        raise FileNotFoundError(f"Mascot image not found: {path}")

    bg = (
        Image.open(path)
        .convert("RGBA")
        .resize((CARD_WIDTH, CARD_HEIGHT))
    )
    overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)

    symbol = signal.get("symbol", "N/A")
    score = float(signal.get("score", 0.0))
    entry = float(signal.get("entry", 0.0))
    sl = signal.get("sl")
    tp = signal.get("tp")
    breakdown = signal.get("breakdown", [])
    flavor = signal.get("flavor", "")
    risk = signal.get("risk", "")
    meta = signal.get("meta_features", {})

    if sl is None or tp is None:
        fallback = await get_last_sl_tp_for_token(symbol)
        sl = sl or fallback.get("sl")
        tp = tp or fallback.get("tp")

    draw.text((30, 30), f"Nova Pulse Signal: {symbol}", font=TITLE_FONT, fill="white")
    draw.text((30, 80), f"Confidence: {score:.2f}", font=TEXT_FONT, fill=(200, 255, 200))
    draw.text((30, 110), f"Entry: {entry:.6f}", font=TEXT_FONT, fill="white")
    draw.text(
        (30, 140),
        f"TP: {float(tp):.6f}" if tp is not None else "TP: ?",
        font=TEXT_FONT,
        fill=(120, 255, 120),
    )
    draw.text(
        (30, 170),
        f"SL: {float(sl):.6f}" if sl is not None else "SL: ?",
        font=TEXT_FONT,
        fill=(255, 120, 120),
    )
    if flavor or risk:
        draw.text(
            (30, 200),
            f"Flavor: {flavor} | Risk: {risk}",
            font=TEXT_FONT,
            fill=(255, 200, 100),
        )

    sources = ", ".join(
        str(item.get("source", "?")) for item in breakdown
    ) or "Unknown"
    draw.text((30, 240), f"Sources: {sources}", font=TEXT_FONT, fill=(180, 200, 255))

    if isinstance(meta, dict) and meta:
        meta_str = ", ".join(f"{k}:{v}" for k, v in meta.items())
        draw.text(
            (30, 270),
            f"Meta: {meta_str}",
            font=TEXT_FONT,
            fill=(180, 210, 220),
        )

    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    draw.text((30, 330), f"Generated: {ts}", font=TEXT_FONT, fill=(140, 140, 140))

    watermark = "@NovaPulseAI"
    w, h = draw.textsize(watermark, font=WATERMARK_FONT)
    draw.text(
        (bg.width - w - 30, bg.height - h - 18),
        watermark,
        font=WATERMARK_FONT,
        fill=(0, 255, 255, 128),
    )

    card = Image.alpha_composite(bg, overlay).convert("RGB")
    output = io.BytesIO()
    card.save(output, format="PNG", optimize=True)
    output.seek(0)
    return output
