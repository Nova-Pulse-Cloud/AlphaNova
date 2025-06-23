# File: src/bot_runner.py

import os
import asyncio
import logging
import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.exceptions import StopPolling

# **Point** at your fixed router
from telegram.integration.telegram_router import router  

# FastAPI app
# Note: uvicorn needs to see your `main_service.py` in the same PYTHONPATH root
APP_MODULE = os.getenv("API_MODULE", "main_service:app")

from core.utils.config import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("nova.start_services")

async def start_api():
    port = int(os.getenv("PORT", "8000"))
    config = uvicorn.Config(
        APP_MODULE,
        host="0.0.0.0",
        port=port,
        log_level="info",
    )
    server = uvicorn.Server(config)
    logger.info("Starting FastAPI on 0.0.0.0:%d", port)
    await server.serve()

async def start_telegram():
    bot = Bot(token=config.TELEGRAM_BOT_TOKEN, parse_mode="Markdown")
    dp  = Dispatcher()
    dp.include_router(router)

    me = await bot.get_me()
    logger.info("Starting Telegram bot polling as @%s", me.username)

    try:
        await dp.start_polling(bot, skip_updates=True)
    except StopPolling:
        pass
    finally:
        await bot.session.close()

def main():
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
