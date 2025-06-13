import io
from PIL import Image, ImageDraw, ImageFont
from datetime import datetime
from core.memory_mesh import get_token_win_rate, get_token_cycle_status

# === Constants ===
BACKGROUND_PATH = "/mnt/data/Nova1.png"
FONT_PATH = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
TITLE_FONT = ImageFont.truetype(FONT_PATH, 24)
TEXT_FONT = ImageFont.truetype(FONT_PATH, 16)

# Cache the background image to improve performance
bg = Image.open(BACKGROUND_PATH).convert("RGBA").resize((800, 400))

async def generate_pnl_card(symbol: str) -> io.BytesIO:
    try:
        # Load and prepare image overlay
        overlay = Image.new("RGBA", bg.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)

        # Fetch data for the card asynchronously
        win_rate = await get_token_win_rate(symbol)
        cycle = await get_token_cycle_status(symbol)

        # Draw text on the overlay
        draw.text((30, 20), f"Nova Pulse - {symbol} PnL Snapshot", font=TITLE_FONT, fill=(255, 255, 255))
        draw.text((30, 100), f"Cycle: {cycle}", font=TEXT_FONT, fill=(220, 255, 255))
        draw.text((30, 140), f"Win Rate: {win_rate * 100:.2f}%", font=TEXT_FONT, fill=(180, 255, 180))
        draw.text((30, 180), f"Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC", font=TEXT_FONT, fill=(180, 180, 180))
        draw.text((30, 230), "@NovaPulseAI", font=TEXT_FONT, fill=(255, 215, 0))

        # Combine background and overlay
        combined = Image.alpha_composite(bg, overlay)
        
        # Prepare image for output
        output = io.BytesIO()
        combined.convert("RGB").save(output, format="PNG")
        output.seek(0)
        return output

    except Exception as e:
        # Error handling for issues with image generation or data fetching
        logger.error(f"Error generating PnL card for symbol {symbol}: {e}")
        fallback_image = Image.new("RGB", (800, 400), (255, 0, 0))  # Fallback to a red image if error occurs
        fallback_image.save(output, format="PNG")
        output.seek(0)
        return output
