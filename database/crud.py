from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence

from sqlalchemy import case, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from database.db import async_session_maker
from database.models import Equipment, EquipmentSpec, SearchHistory, User
from utils.logger import logger


async def get_user(telegram_id: int) -> Optional[User]:
    async with async_session_maker() as session:
        result = await session.execute(
            select(User).where(User.telegram_id == telegram_id)
        )
        return result.scalar_one_or_none()


async def create_user(
    telegram_id: int,
    username: Optional[str] = None,
    full_name: Optional[str] = None,
    is_admin: bool = False,
) -> User:
    async with async_session_maker() as session:
        async with session.begin():
            user = User(
                telegram_id=telegram_id,
                username=username,
                full_name=full_name,
                is_admin=is_admin,
            )
            session.add(user)
        await session.refresh(user)
        logger.info(f"Created user {telegram_id} ({username})")
        return user


async def get_all_equipment() -> Sequence[Equipment]:
    async with async_session_maker() as session:
        result = await session.execute(select(Equipment))
        return result.scalars().all()


async def get_equipment_by_category(category: str) -> Sequence[Equipment]:
    async with async_session_maker() as session:
        result = await session.execute(
            select(Equipment).where(Equipment.category.ilike(category))
        )
        return result.scalars().all()


async def get_equipment_by_name(model_name: str) -> Sequence[Equipment]:
    """Поиск оборудования по названию модели. Точные совпадения идут первыми."""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Equipment)
            .where(Equipment.model_name.ilike(f"%{model_name}%"))
            .order_by(
                case(
                    (func.lower(Equipment.model_name) == model_name.lower(), 0),
                    else_=1,
                )
            )
        )
        return result.scalars().all()


async def get_equipment_count() -> int:
    async with async_session_maker() as session:
        result = await session.execute(select(func.count(Equipment.id)))
        return result.scalar_one()


async def get_stats() -> Dict[str, int]:
    """Количество оборудования по категориям."""
    async with async_session_maker() as session:
        result = await session.execute(
            select(Equipment.category, func.count(Equipment.id))
            .group_by(Equipment.category)
            .order_by(Equipment.category)
        )
        return {row[0]: row[1] for row in result.all()}


async def bulk_create_equipment(items: List[Dict[str, Any]]) -> int:
    """Массовая вставка оборудования без характеристик. Возвращает число вставленных строк."""
    if not items:
        return 0
    async with async_session_maker() as session:
        async with session.begin():
            session.add_all([Equipment(**data) for data in items])
        logger.info(f"Bulk inserted {len(items)} equipment records")
        return len(items)


async def bulk_create_equipment_with_specs(records: List[Dict[str, Any]]) -> int:
    """
    Insert equipment rows and their EAV specs in one transaction.

    records: list of dicts:
        {
            "model_name": str,
            "category": str,
            "version": str | None,
            "source_filename": str,
            "specs": [(char_name, value_text, value_num, canonical_name), ...]
        }

    Returns number of inserted equipment rows.
    """
    if not records:
        return 0

    async with async_session_maker() as session:
        async with session.begin():
            for record in records:
                specs_data = record.get("specs", [])
                eq_data = {k: v for k, v in record.items() if k != "specs"}
                eq = Equipment(**eq_data)
                session.add(eq)
                await session.flush()  # populate eq.id

                for spec_row in specs_data:
                    # Поддержка 3-кортежа (устаревший) и 4-кортежа (с canonical_name)
                    if len(spec_row) == 4:
                        char_name, value_text, value_num, canonical_name = spec_row
                    else:
                        char_name, value_text, value_num = spec_row
                        canonical_name = None
                    session.add(EquipmentSpec(
                        equipment_id=eq.id,
                        char_name=char_name,
                        canonical_name=canonical_name,
                        value_text=value_text,
                        value_num=value_num,
                    ))

    logger.info(f"Bulk inserted {len(records)} equipment records with specs")
    return len(records)


async def get_specs_by_equipment_ids(
    equipment_ids: List[int],
) -> Dict[int, List[EquipmentSpec]]:
    """Вернуть все EquipmentSpec для указанных ID оборудования, сгруппированные по equipment_id."""
    if not equipment_ids:
        return {}

    async with async_session_maker() as session:
        result = await session.execute(
            select(EquipmentSpec).where(
                EquipmentSpec.equipment_id.in_(equipment_ids)
            )
        )
        specs = result.scalars().all()

    grouped: Dict[int, List[EquipmentSpec]] = defaultdict(list)
    for spec in specs:
        grouped[spec.equipment_id].append(spec)
    return dict(grouped)


async def find_matching_equipment_by_canonical(
    category: str,
    canonical_reqs: List[tuple],  # [(canonical_name, num_val, op), ...]
) -> set:
    """
    SQL: найти equipment_id, удовлетворяющие числовым требованиям по canonical_name.
    canonical_reqs: list of (canonical_name: str, num_val: float, op: str)
    """
    # Операторы через lambda, без f-строк — защита от SQL injection
    _OP_FUNCS = {
        ">=": lambda col, val: col >= val,
        "<=": lambda col, val: col <= val,
        ">":  lambda col, val: col > val,
        "<":  lambda col, val: col < val,
        "=":  lambda col, val: col == val,
    }
    if not canonical_reqs:
        return set()

    async with async_session_maker() as session:
        result_set: Optional[set] = None
        for canonical_name, num_val, op in canonical_reqs:
            op_func = _OP_FUNCS.get(op, _OP_FUNCS[">="])
            rows = await session.execute(
                select(EquipmentSpec.equipment_id.distinct())
                .join(Equipment, Equipment.id == EquipmentSpec.equipment_id)
                .where(
                    Equipment.category.ilike(category),
                    EquipmentSpec.canonical_name == canonical_name,
                    op_func(EquipmentSpec.value_num, num_val),
                )
            )
            ids = {r[0] for r in rows}
            result_set = ids if result_set is None else result_set & ids
        return result_set or set()


async def delete_all_equipment() -> int:
    """Удалить всё оборудование из БД (характеристики удаляются по CASCADE). Возвращает число строк."""
    async with async_session_maker() as session:
        async with session.begin():
            result = await session.execute(text("DELETE FROM equipment"))
            count = result.rowcount
        logger.info(f"Deleted {count} equipment records (specs deleted via CASCADE)")
        return count



async def get_models_count() -> int:
    """Псевдоним get_equipment_count() для обратной совместимости."""
    return await get_equipment_count()




async def save_search_history(
    user_id: int,
    docx_filename: str,
    requirements: Optional[Dict] = None,
    results_summary: Optional[Dict] = None,
) -> SearchHistory:
    async with async_session_maker() as session:
        async with session.begin():
            record = SearchHistory(
                user_id=user_id,
                docx_filename=docx_filename,
                requirements=requirements,
                results_summary=results_summary,
            )
            session.add(record)
        await session.refresh(record)
        logger.info(f"Saved search history for user {user_id}: {docx_filename}")
        return record
