import io
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from core.sl_manager import get_last_sl_tp_for_token

# === Constants ===
BACKGROUND_PATH = "/mnt/data/Nova1.png"
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT = ImageFont.truetype(FONT_PATH, 26)
TEXT_FONT = ImageFont.truetype(FONT_PATH, 16)

async def generate_signal_card(signal: dict) -> io.BytesIO:
    """
    Nova Pulse - Signal Card Renderer with enhanced error handling, branding, and SL/TP fallback.
    Generates an image representing the trading signal, including entry, stop loss, take profit, and agent breakdown.
    """
    try:
        # Load and prepare image background
        bg = Image.open(BACKGROUND_PATH).convert("RGBA").resize((800, 400))
        overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # Extract signal details
        symbol = signal.get("symbol", "N/A")
        score = float(signal.get("score", 0.0))
        entry = float(signal.get("entry", 0.0))
        sl = signal.get("sl")
        tp = signal.get("tp")
        breakdown = signal.get("breakdown", [])

        # === Fallback for missing SL/TP ===
        if not sl or not tp:
            fallback = await get_last_sl_tp_for_token(symbol)
            sl = sl or fallback.get("sl")
            tp = tp or fallback.get("tp")

        # === Draw Text on the Signal Card ===
        draw.text((30, 20), f"Nova Pulse Signal: {symbol}", font=TITLE_FONT, fill=(255, 255, 255))
        draw.text((30, 80), f"Confidence Score: {score:.2f}", font=TEXT_FONT, fill=(200, 255, 200))
        draw.text((30, 110), f"Entry Price: {entry:.4f}", font=TEXT_FONT, fill=(255, 255, 255))

        # Fallback for missing SL/TP with conditional coloring
        if sl:
            draw.text((30, 140), f"Stop Loss: {float(sl):.4f}", font=TEXT_FONT, fill=(255, 120, 120))
        if tp:
            draw.text((30, 170), f"Take Profit: {float(tp):.4f}", font=TEXT_FONT, fill=(120, 255, 120))

        # Fused agent breakdown - displaying the sources of the signal
        agent_list = ", ".join([b.get("source", "?") for b in breakdown]) or "Unknown"
        draw.text((30, 220), f"Fused From: {agent_list}", font=TEXT_FONT, fill=(180, 200, 255))

        # Generated timestamp
        draw.text((30, 260), f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC", font=TEXT_FONT, fill=(140, 140, 140))

        # Branding
        draw.text((30, 310), "@NovaPulseAI", font=TEXT_FONT, fill=(255, 215, 0))

        # Combine background and overlay for final image
        combined = Image.alpha_composite(bg, overlay)

        # Prepare in-memory PNG output
        output = io.BytesIO()
        combined.convert("RGB").save(output, format="PNG")
        output.seek(0)
        return output

    except Exception as e:
        # Log any errors that occur during image generation
        logger.error(f"Failed to generate signal card: {e}")
        raise
