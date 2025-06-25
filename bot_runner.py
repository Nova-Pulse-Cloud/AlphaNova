# File: src/bot_runner.py

import os
import asyncio
import logging

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.exceptions import StopPolling

from core.utils.config import config
from telegram.integration.telegram_router import router

# FastAPI app; uvicorn will look for this module:app
APP_MODULE = os.getenv("API_MODULE", "main_service:app")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("nova.start_services")


async def start_api() -> None:
    """Launch the FastAPI app via Uvicorn."""
    port = int(os.getenv("PORT", "8000"))
    uv_config = uvicorn.Config(
        APP_MODULE,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(uv_config)
    logger.info("Starting FastAPI on 0.0.0.0:%d", port)
    await server.serve()


async def start_telegram() -> None:
    """Start Aiogram polling for Telegram updates."""
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN, parse_mode="Markdown")
    dp = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    logger.info("Starting Telegram bot polling as @%s", me.username)

    try:
        await dp.start_polling(bot, skip_updates=True)
    except StopPolling as e:
        logger.info("Polling stopped: %s", e)
    finally:
        await bot.session.close()


def main() -> None:
    """Run both API server and Telegram polling concurrently."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    tasks = [
        loop.create_task(start_api()),
        loop.create_task(start_telegram()),
    ]
    try:
        loop.run_until_complete(asyncio.gather(*tasks))
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down services…")
    finally:
        loop.stop()
        loop.close()


if __name__ == "__main__":
    main()
