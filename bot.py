import asyncio
import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from config import settings
from handlers import document, start, stats
from middleware.auth import AuthMiddleware
from utils.logger import logger


def _run_migrations() -> None:
    """Run Alembic database migrations on startup."""
    try:
        from alembic import command
        from alembic.config import Config
        alembic_cfg = Config("alembic.ini")
        command.upgrade(alembic_cfg, "head")
        logger.info("Database migrations applied successfully")
    except Exception as e:
        logger.error(f"Failed to run database migrations: {e}")
        raise


async def main() -> None:
    logger.info("Starting tender matcher bot...")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _run_migrations)

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp = Dispatcher()

    dp.message.middleware(AuthMiddleware())

    dp.include_router(start.router)
    dp.include_router(stats.router)
    dp.include_router(document.router)

    logger.info("Bot started. Polling...")
    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()
        logger.info("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
