"""Handler for /stats command — shows equipment DB statistics."""

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from database.crud import get_stats
from utils.logger import logger

router = Router()


@router.message(Command("stats"))
async def stats_handler(message: Message) -> None:
    """Show equipment count grouped by category."""
    logger.info(f"/stats from {message.from_user.id}")
    try:
        stats = await get_stats()
    except Exception as e:
        logger.error(f"Failed to get stats: {e}")
        await message.answer("Не удалось получить статистику. Попробуйте позже.")
        return

    if not stats:
        await message.answer("База данных пуста.")
        return

    total = sum(stats.values())
    lines = ["📊 <b>Статистика базы данных:</b>\n"]
    for category, count in sorted(stats.items()):
        lines.append(f"• {category}: <b>{count}</b>")
    lines.append(f"\n<b>Всего моделей: {total}</b>")

    await message.answer("\n".join(lines), parse_mode="HTML")
