from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import Message, TelegramObject

from config import settings
from database.crud import create_user, get_user
from utils.logger import logger


class AuthMiddleware(BaseMiddleware):
    """
    Whitelist-мидлвар. Алгоритм:
    1. Проверяем telegram_id в таблице users.
    2. Если есть и is_admin=True → пропускаем.
    3. Если нет в БД → проверяем ADMIN_IDS из .env.
    4. Если найден в .env → создаём запись с is_admin=True, пропускаем.
    5. Иначе → запрещаем доступ.
    """

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if not isinstance(event, Message):
            return await handler(event, data)

        user = event.from_user
        if user is None:
            return

        telegram_id = user.id

        db_user = await get_user(telegram_id)

        if db_user and db_user.is_admin:
            return await handler(event, data)

        if db_user is None and telegram_id in settings.admin_ids_list:
            full_name = user.full_name or ""
            await create_user(
                telegram_id=telegram_id,
                username=user.username,
                full_name=full_name,
                is_admin=True,
            )
            logger.info(f"Автоматически зарегистрирован администратор {telegram_id} ({user.username})")
            return await handler(event, data)

        logger.warning(f"Доступ запрещён для {telegram_id} ({user.username})")
        await event.answer(
            f"Доступ запрещён. Ваш ID: {telegram_id}\n"
            "Обратитесь к администратору для получения доступа."
        )
        return None
