# File: start_services.py
import os
import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.exceptions import StopPolling
from telegram.integration.telegram_router import router
import uvicorn

# Pull in the same config you use everywhere
from core.utils.config import config

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("nova.start_services")

async def start_api():
    """
    Launches the FastAPI service via Uvicorn programmatically.
    """
    port = int(os.getenv("PORT", "8000"))
    uv_config = uvicorn.Config(
        "main_service:app",
        host="0.0.0.0",
        port=port,
        log_level="info",
        loop="asyncio",
        proxy_headers=True,
        ws_max_size=2**20,
    )
    server = uvicorn.Server(uv_config)
    logger.info("Starting FastAPI on 0.0.0.0:%d", port)
    # this will block until shutdown
    await server.serve()

async def start_telegram():
    """
    Launches the Aiogram Telegram bot polling loop.
    """
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN, parse_mode="Markdown")
    dp  = Dispatcher()
    dp.include_router(router)

    logger.info("Starting Telegram bot polling as @%s", (await bot.get_me()).username)

    try:
        # skip_updates=True discards any backlog on startup
        await dp.start_polling(bot, skip_updates=True)
    except StopPolling:
        # Graceful shutdown requested
        pass
    finally:
        await bot.session.close()

def main():
    """
    Entry point: run both services concurrently in asyncio.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    tasks = [
        loop.create_task(start_api()),
        loop.create_task(start_telegram()),
    ]
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down services...")
    finally:
        loop.stop()
        loop.close()

if __name__ == "__main__":
    main()
