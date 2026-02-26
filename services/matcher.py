"""
Модуль для сопоставления требований тендера с оборудованием в БД.

Использует нечёткое сопоставление (difflib.SequenceMatcher) для сравнения
оригинальных названий характеристик из ТЗ с оригинальными названиями из БД.
Это устраняет зависимость от нормализации ключей.

Основные функции:
- find_matching_models: главная функция поиска и сопоставления
- calculate_match_percentage_fuzzy: вычисление % совпадения через fuzzy match
- find_best_char_match: поиск наиболее похожей характеристики по названию
- compare_values_eav: сравнение значений (числовое + текстовое)
"""

import asyncio
import functools
import json
import re
import time
from collections import defaultdict
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from config import settings
from database.crud import (
    get_equipment_by_name,
    get_equipment_by_category,
    get_all_equipment,
    get_specs_by_equipment_ids,
)
from database.models import Equipment, EquipmentSpec
from utils.logger import logger


# Mapping категорий к подкатегориям для расширенного поиска
CATEGORY_SUBCATEGORIES: Dict[str, List[str]] = {
    "Коммутаторы": ["Управляемый", "Неуправляемый", "Промышленный"],
    "Маршрутизаторы": ["Универсальный шлюз безопасности", "Модульный"],
}

# Пороги fuzzy match
CHAR_SIMILARITY_THRESHOLD = 0.6   # схожесть названий характеристик
VALUE_TEXT_SIMILARITY_THRESHOLD = 0.7  # схожесть текстовых значений


# ════════════════════════════════════════════════════════════════════════════
# Извлечение чисел и операторов
# ════════════════════════════════════════════════════════════════════════════


_UNIT_MULTIPLIERS = [
    (re.compile(r'тбит|tbps', re.IGNORECASE), 1_000_000),
    (re.compile(r'гбит|gbps', re.IGNORECASE), 1_000),
    (re.compile(r'мбит|mbps', re.IGNORECASE), 1),
    (re.compile(r'кбит|kbps', re.IGNORECASE), 0.001),
    (re.compile(r'\btb\b|\bтб\b', re.IGNORECASE), 1_048_576),
    (re.compile(r'\bgb\b|\bгб\b', re.IGNORECASE), 1_024),
    (re.compile(r'\bmb\b|\bмб\b', re.IGNORECASE), 1),
    (re.compile(r'\bkb\b|\bкб\b', re.IGNORECASE), 0.001),
]

# Matches individual operator+number tokens inside compound conditions like "> 32 и <= 64"
_COMPOUND_RE = re.compile(
    r'(?:[≥≤><≠=]+|>=|<=|!=|не\s+менее|не\s+более|до)\s*[\d,.]+',
    re.IGNORECASE,
)


def _apply_unit_multiplier(val_str: str, number: float) -> float:
    for pattern, multiplier in _UNIT_MULTIPLIERS:
        if pattern.search(val_str):
            return number * multiplier
    return number


def extract_number(val) -> Optional[float]:
    """
    Извлечение числового значения из различных форматов.

    Поддерживаемые форматы:
    - Простые числа: 24, 200.5, -40
    - Строки с единицами: "24 порта", "200 Вт", "2 ГБ"
    - Дробные числа: "1.5 Гбит/с", "2,5 ГБ"
    - Диапазоны: "10-20" → 20 (максимум)
    - Умножение: "2x4" → 8
    - Сложение: "24+4" → 28
    - Префиксы: "до 1000" → 1000, "не менее 500" → 500
    - Единицы скорости: "10 Gbps" → 10000 (Мбит)
    - Единицы памяти: "2 GB" → 2048 (МБ)
    """
    if isinstance(val, bool):
        return None

    if isinstance(val, (int, float)):
        return float(val)

    if not isinstance(val, str):
        return None

    original = val.strip()
    val_clean = re.sub(r'^[≥≤><≠=]+\s*', '', original)
    val_normalized = val_clean.replace(',', '.')

    # Сложение: "24+4" → 28
    sum_match = re.match(r'^(\d+(?:\.\d+)?)\s*\+\s*(\d+(?:\.\d+)?)$', val_normalized.strip())
    if sum_match:
        return _apply_unit_multiplier(original, float(sum_match.group(1)) + float(sum_match.group(2)))

    # Диапазоны: берём максимум
    range_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:-|до)\s*(\d+(?:\.\d+)?)', val_normalized)
    if range_match:
        return _apply_unit_multiplier(original, max(float(range_match.group(1)), float(range_match.group(2))))

    # Умножение: "2x4"
    mult_match = re.search(r'(\d+)\s*(?:[xхX×]|блок\w*\s+по)\s*(\d+)', val_normalized, re.IGNORECASE)
    if mult_match:
        return _apply_unit_multiplier(original, float(mult_match.group(1)) * float(mult_match.group(2)))

    # Префиксы
    prefix_match = re.search(r'(?:до|не\s+менее|минимум|максимум)\s+(\d+(?:\.\d+)?)', val_normalized, re.IGNORECASE)
    if prefix_match:
        return _apply_unit_multiplier(original, float(prefix_match.group(1)))

    # Простое число
    match = re.search(r"[-+]?\d*\.?\d+", val_normalized)
    if match:
        return _apply_unit_multiplier(original, float(match.group()))

    return None


def extract_number_with_operator(val) -> Tuple[Optional[float], str]:
    """
    Извлечение числового значения и оператора сравнения из значения ТЗ.

    Returns:
        Tuple (number, operator):
        - "≥ 24" → (24.0, ">=")
        - "24" → (24.0, ">=")  (дефолт — модель должна >= требования)
    """
    default_op = ">="

    if val is None or isinstance(val, bool):
        return (None, default_op)

    if isinstance(val, (int, float)):
        return (float(val), default_op)

    if not isinstance(val, str):
        return (None, default_op)

    val_stripped = val.strip()
    op = default_op

    operator_patterns = [
        (r'^>=\s*', ">="),
        (r'^≥\s*', ">="),
        (r'^<=\s*', "<="),
        (r'^≤\s*', "<="),
        (r'^!=\s*', "!="),
        (r'^≠\s*', "!="),
        (r'^>\s*', ">"),
        (r'^<\s*', "<"),
        (r'^=\s*', "="),
    ]

    for pattern, operator in operator_patterns:
        if re.match(pattern, val_stripped):
            op = operator
            break

    text_prefix_patterns = [
        (r'не\s+менее', ">="),
        (r'не\s+более', "<="),
        (r'минимум', ">="),
        (r'максимум', "<="),
        (r'^до\s+', "<="),
    ]

    for pattern, operator in text_prefix_patterns:
        if re.search(pattern, val_stripped, re.IGNORECASE):
            op = operator
            break

    number = extract_number(val)
    return (number, op)


# ════════════════════════════════════════════════════════════════════════════
# Текстовое сравнение
# ════════════════════════════════════════════════════════════════════════════


_YES_SYNONYMS = {'да', 'yes', 'есть', 'имеется', 'поддерживается', 'true', '1'}
_NO_SYNONYMS = {'нет', 'no', 'отсутствует', 'не поддерживается', 'false', '0'}


def compare_text_values(required: str, model: str) -> bool:
    """
    Многоуровневое текстовое сравнение.

    1. Точное совпадение (case-insensitive)
    2. Boolean-семантика
    3. Частичное совпадение на уровне слов (req_words ⊆ mod_words)
    4. Пересечение comma-separated списков
    5. Fuzzy сравнение (ratio >= 0.7)
    """
    req = required.strip().lower()
    mod = model.strip().lower()

    if req == mod:
        return True

    if req in _YES_SYNONYMS and mod in _YES_SYNONYMS:
        return True
    if req in _NO_SYNONYMS and mod in _NO_SYNONYMS:
        return True

    req_words = set(re.split(r'[\s,;/]+', req)) - {''}
    mod_words = set(re.split(r'[\s,;/]+', mod)) - {''}
    if req_words and mod_words:
        if req_words <= mod_words:
            return True

    req_parts = {p.strip() for p in req.split(',')}
    mod_parts = {p.strip() for p in mod.split(',')}
    if len(req_parts) > 1 or len(mod_parts) > 1:
        if req_parts & mod_parts:
            return True

    # Guard: if strings contain digit-bearing tokens and they differ, don't fuzzy-match.
    # Prevents false positives like "Layer 3" ≈ "Layer 2" or "Управляемый L3" ≈ "Управляемый".
    req_digit_tokens = set(re.findall(r'\b\w*\d\w*\b', req))
    mod_digit_tokens = set(re.findall(r'\b\w*\d\w*\b', mod))
    if req_digit_tokens != mod_digit_tokens:
        return False

    ratio = SequenceMatcher(None, req, mod).ratio()
    if ratio >= VALUE_TEXT_SIMILARITY_THRESHOLD:
        return True

    return False


# ════════════════════════════════════════════════════════════════════════════
# Compound condition helpers
# ════════════════════════════════════════════════════════════════════════════


def _parse_compound_conditions(text: str) -> Optional[List[Tuple[float, str]]]:
    """
    Parse compound condition text like "> 32 и <= 64" into [(32.0, ">"), (64.0, "<=")].

    Returns None if fewer than 2 operator+number tokens are found.
    """
    parts = _COMPOUND_RE.findall(text)
    if len(parts) < 2:
        return None
    result = []
    for p in parts:
        num, op = extract_number_with_operator(p.strip())
        if num is not None:
            result.append((num, op))
    return result if len(result) >= 2 else None


def _compound_conditions_compatible(
    tz_conds: List[Tuple[float, str]],
    db_conds: List[Tuple[float, str]],
    allow_lower: bool,
) -> bool:
    """
    Range containment check: DB range must be at least as wide as TZ requirement.

    - '>' / '>=': DB lower bound <= TZ lower bound  (model covers more from below)
    - '<' / '<=': DB upper bound >= TZ upper bound  (model covers more from above)
    - '=': approximately equal
    """
    def _is_lower(op: str) -> bool:
        return op in (">", ">=")

    def _is_upper(op: str) -> bool:
        return op in ("<", "<=")

    tz_lower = [(n, op) for n, op in tz_conds if _is_lower(op)]
    tz_upper = [(n, op) for n, op in tz_conds if _is_upper(op)]
    db_lower = [(n, op) for n, op in db_conds if _is_lower(op)]
    db_upper = [(n, op) for n, op in db_conds if _is_upper(op)]

    for tz_n, _ in tz_lower:
        if not db_lower:
            return False
        best_db_n = min(x[0] for x in db_lower)
        tol = tz_n * 0.05 if allow_lower else 0.0
        if best_db_n > tz_n + tol:
            return False

    for tz_n, _ in tz_upper:
        if not db_upper:
            return False
        best_db_n = max(x[0] for x in db_upper)
        tol = tz_n * 0.05 if allow_lower else 0.0
        if best_db_n < tz_n - tol:
            return False

    return True


# ════════════════════════════════════════════════════════════════════════════
# Сравнение значений (EAV)
# ════════════════════════════════════════════════════════════════════════════


def compare_values_eav(
    req_value: Any,
    value_text: Optional[str],
    value_num: Optional[float],
    allow_lower: bool = False,
) -> bool:
    """
    Сравнение требуемого значения с фактическим из equipment_specs.

    - Boolean: сравнение с текстом
    - Числовые: математическое сравнение с оператором
    - Строковые: compare_text_values
    """
    # Bug 2 fix: only bail out when both storage columns are empty
    if value_text is None and value_num is None:
        return False

    # Bug 1 fix: compound conditions like "> 32 и <= 64"
    if isinstance(req_value, list):
        if value_text:
            db_conds = _parse_compound_conditions(value_text)
            if db_conds:
                tz_conds = []
                for v in req_value:
                    num, op = extract_number_with_operator(v)
                    if num is not None:
                        tz_conds.append((num, op))
                if tz_conds and _compound_conditions_compatible(tz_conds, db_conds, allow_lower):
                    return True
        return all(compare_values_eav(v, value_text, value_num, allow_lower) for v in req_value)

    if isinstance(req_value, bool):
        if value_text is None:
            return False
        req_str = "да" if req_value else "нет"
        return compare_text_values(req_str, value_text)

    req_num, op = extract_number_with_operator(req_value)
    model_num = extract_number(value_text) if value_text else value_num
    # Bug 2 fix: compound DB value like ">=32 и <=64" — pick the appropriate bound.
    # For >= / > (TZ needs at-least X): check DB's upper bound.
    # For <= / < (TZ needs at-most X): check DB's lower bound.
    if req_num is not None and value_text:
        db_conds = _parse_compound_conditions(value_text)
        if db_conds:
            if op in (">=", ">"):
                upper_vals = [n for n, dop in db_conds if dop in ("<", "<=")]
                model_num = max(upper_vals) if upper_vals else max(n for n, _ in db_conds)
            elif op in ("<=", "<"):
                lower_vals = [n for n, dop in db_conds if dop in (">", ">=")]
                model_num = min(lower_vals) if lower_vals else min(n for n, _ in db_conds)
    if req_num is not None and model_num is not None:
        result = _apply_operator(req_num, model_num, op, allow_lower)
        logger.debug(
            f"Numeric compare: req={req_num} {op} model_num={model_num} → {result}"
        )
        return result

    if isinstance(req_value, str):
        if value_text is None:
            return False
        return compare_text_values(req_value, value_text)

    return str(req_value) == value_text


def _apply_operator(req_num: float, model_num: float, op: str, allow_lower: bool) -> bool:
    if op == ">=":
        return model_num >= (req_num * 0.95 if allow_lower else req_num)
    elif op == "<=":
        return model_num <= (req_num * 1.05 if allow_lower else req_num)
    elif op == "=":
        # Use relative tolerance for large values, absolute for small ones
        denom = max(abs(req_num), abs(model_num))
        if denom > 1:
            return abs(model_num - req_num) / denom < 0.001
        return abs(model_num - req_num) < 0.01
    elif op == "!=":
        denom = max(abs(req_num), abs(model_num))
        if denom > 1:
            return abs(model_num - req_num) / denom >= 0.001
        return abs(model_num - req_num) >= 0.01
    elif op == ">":
        return model_num > (req_num * 0.95 if allow_lower else req_num)
    elif op == "<":
        return model_num < (req_num * 1.05 if allow_lower else req_num)
    else:
        return model_num >= req_num


# ════════════════════════════════════════════════════════════════════════════
# Fuzzy matching характеристик
# ════════════════════════════════════════════════════════════════════════════


def _char_similarity(a: str, b: str) -> float:
    """Схожесть двух названий характеристик (0..1)."""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def find_best_char_match(
    req_char: str,
    specs: List[EquipmentSpec],
) -> Optional[EquipmentSpec]:
    """
    Найти наиболее похожую характеристику по названию из списка specs.

    Returns:
        EquipmentSpec с наибольшим ratio, если ratio >= CHAR_SIMILARITY_THRESHOLD.
        None если ни одна характеристика не набрала порог.
    """
    best: Optional[EquipmentSpec] = None
    best_ratio = 0.0

    for spec in specs:
        ratio = _char_similarity(req_char, spec.char_name)
        if ratio > best_ratio:
            best_ratio = ratio
            best = spec

    if best_ratio >= CHAR_SIMILARITY_THRESHOLD:
        logger.debug(
            f"Char match: '{req_char}' → '{best.char_name}' (ratio={best_ratio:.2f})"
        )
        return best

    logger.debug(f"No char match for '{req_char}' (best_ratio={best_ratio:.2f})")
    return None


# ════════════════════════════════════════════════════════════════════════════
# Быстрое сопоставление: precomputed char_mapping
# ════════════════════════════════════════════════════════════════════════════


def _build_char_mapping(
    required_chars: List[str],
    all_char_names: Set[str],
) -> Dict[str, str]:
    """
    Однократно вычислить маппинг req_char → лучший db_char_name.

    Сложность: O(len(req_chars) × len(unique_db_chars))
    Вместо O(N_candidates × len(req_chars) × avg_specs_per_candidate).

    Пример: 98 × 260 = 25K вместо 365 × 98 × 260 = 9.3M вызовов SequenceMatcher.
    """
    mapping: Dict[str, str] = {}
    for req_char in required_chars:
        best_name: Optional[str] = None
        best_ratio = 0.0
        req_lower = req_char.lower().strip()
        for db_char in all_char_names:
            ratio = SequenceMatcher(None, req_lower, db_char.lower().strip()).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_name = db_char
        if best_ratio >= CHAR_SIMILARITY_THRESHOLD and best_name is not None:
            mapping[req_char] = best_name
            logger.debug(f"Char mapping: '{req_char}' → '{best_name}' ({best_ratio:.2f})")
        else:
            logger.debug(f"No mapping for '{req_char}' (best={best_ratio:.2f})")
    return mapping


async def _build_char_mapping_llm(
    required_chars: List[str],
    all_char_names: Set[str],
) -> Dict[str, str]:
    """
    Semantic char mapping via OpenAI GPT-4o-mini.

    Falls back to SequenceMatcher if key is missing or the API call fails.
    Hallucination guard: only mappings where the suggested DB name actually
    exists in all_char_names are accepted.
    """
    if not settings.openai_api_key or not settings.llm_char_matching_enabled:
        return _build_char_mapping(required_chars, all_char_names)

    try:
        from openai import AsyncOpenAI
        client = AsyncOpenAI(api_key=settings.openai_api_key)

        db_names_list = sorted(all_char_names)
        prompt = (
            f"TZ characteristic names (from tender): {required_chars}\n"
            f"DB characteristic names (from database): {db_names_list}\n\n"
            "Return a JSON object mapping each TZ name to the semantically equivalent "
            "DB name, or null if no match exists.\n"
            "Format: {\"<tz_char>\": \"<db_char>\" or null, ...}\n"
            "Consider synonyms, abbreviations, mixed Russian/English, and unit equivalences "
            "(e.g. Гбит/с = 1000 Мбит/с). Return null when unsure."
        )

        resp = await client.chat.completions.create(
            model=settings.openai_router_model,
            response_format={"type": "json_object"},
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a technical specification mapper. "
                        "Map characteristic names semantically between two lists."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )

        data = json.loads(resp.choices[0].message.content)

        mapping: Dict[str, str] = {}
        for tz_char, db_char in data.items():
            if db_char is not None and db_char in all_char_names:
                mapping[tz_char] = db_char
                logger.debug(f"LLM char mapping: '{tz_char}' → '{db_char}'")
            elif db_char is not None:
                logger.debug(
                    f"LLM char mapping rejected (not in DB): '{tz_char}' → '{db_char}'"
                )

        logger.info(
            f"LLM char mapping: {len(mapping)}/{len(required_chars)} mapped "
            f"(model={settings.openai_router_model})"
        )
        return mapping

    except Exception as e:
        logger.warning(f"LLM char mapping failed, falling back to SequenceMatcher: {e}")
        return _build_char_mapping(required_chars, all_char_names)


def _match_one_model(
    required_specs: Dict[str, Any],
    eq_specs_dict: Dict[str, EquipmentSpec],
    char_mapping: Dict[str, str],
    allow_lower: bool = False,
    canonical_map: Optional[Dict[str, str]] = None,
    eq_canonical_dict: Optional[Dict[str, EquipmentSpec]] = None,
) -> Dict[str, Any]:
    """
    Вычислить % совпадения для одной модели, используя precomputed char_mapping.

    eq_specs_dict:     {char_name: EquipmentSpec}     — O(1) fuzzy fallback.
    char_mapping:      {req_char: db_char_name}        — precomputed fuzzy.
    canonical_map:     {req_char: canonical_name}      — from LLM parser (exact match).
    eq_canonical_dict: {canonical_name: EquipmentSpec} — O(1) exact canonical lookup.
    """
    if not required_specs:
        return {
            "match_percentage": 100.0,
            "matched_specs": [], "unmapped_specs": [], "missing_specs": [],
            "different_specs": {}, "matched_values": {},
        }

    total = len(required_specs)
    matched_count = 0
    matched_specs: List[str] = []
    unmapped_specs: List[str] = []   # req_char found nowhere in DB (no fuzzy/canonical match globally)
    missing_specs: List[str] = []    # req_char found in DB globally, but absent in this specific model
    different_specs: Dict[str, tuple] = {}
    matched_values: Dict[str, str] = {}

    for req_char, req_value in required_specs.items():
        # Skip the internal canonical map key
        if req_char == "__canonical__":
            total -= 1
            continue

        spec: Optional[EquipmentSpec] = None

        # 1. Exact canonical lookup (from LLM parser)
        if canonical_map and eq_canonical_dict:
            cname = canonical_map.get(req_char)
            if cname:
                spec = eq_canonical_dict.get(cname)

        # 2. Fuzzy fallback via precomputed char_mapping
        globally_mapped = False
        if spec is None:
            db_char_name = char_mapping.get(req_char)
            if db_char_name:
                globally_mapped = True
                spec = eq_specs_dict.get(db_char_name)

        if spec is None:
            # Distinguish: known in DB (but not in this model) vs unknown in DB entirely
            if globally_mapped:
                missing_specs.append(req_char)
            else:
                unmapped_specs.append(req_char)
            continue

        matched_values[req_char] = spec.value_text or ""

        if compare_values_eav(req_value, spec.value_text, spec.value_num, allow_lower):
            matched_count += 1
            matched_specs.append(req_char)
        else:
            req_display = " И ".join(str(v) for v in req_value) if isinstance(req_value, list) else req_value
            different_specs[req_char] = (req_display, spec.value_text)

    if total <= 0:
        return {
            "match_percentage": 100.0,
            "matched_specs": [], "unmapped_specs": [], "missing_specs": [],
            "different_specs": {}, "matched_values": {},
        }

    return {
        "match_percentage": round((matched_count / total) * 100.0, 2),
        "matched_specs": matched_specs,
        "unmapped_specs": unmapped_specs,
        "missing_specs": missing_specs,
        "different_specs": different_specs,
        "matched_values": matched_values,
    }


def _run_matching_sync(
    required_specs: Dict[str, Any],
    candidates_data: List[Dict[str, Any]],
    specs_by_id: Dict[int, List[EquipmentSpec]],
    allow_lower: bool,
    precomputed_char_mapping: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    """
    CPU-bound часть матчинга — запускается в executor чтобы не блокировать event loop.

    candidates_data: [{id, model_name, category, version, source_filename}, ...]
    precomputed_char_mapping: if provided (built async by LLM), skip SequenceMatcher step.
    """
    # Извлечь canonical_map из required_specs (добавляется LLM парсером)
    canonical_map: Optional[Dict[str, str]] = required_specs.get("__canonical__")
    # Фильтруем __canonical__ из specs для матчинга
    effective_specs = {k: v for k, v in required_specs.items() if k != "__canonical__"}

    # Шаг 1: собрать все уникальные char_names из всех кандидатов
    all_char_names: Set[str] = set()
    for specs in specs_by_id.values():
        for spec in specs:
            all_char_names.add(spec.char_name)

    logger.info(
        f"Unique char_names in DB: {len(all_char_names)}, "
        f"required: {len(effective_specs)}, candidates: {len(candidates_data)}, "
        f"canonical_map: {len(canonical_map) if canonical_map else 0} entries"
    )

    # Шаг 2: use precomputed LLM mapping if provided, else fall back to SequenceMatcher
    if precomputed_char_mapping is not None:
        char_mapping = precomputed_char_mapping
        logger.info(
            f"Using precomputed LLM char mapping: "
            f"{len(char_mapping)}/{len(effective_specs)} mapped"
        )
    else:
        t0 = time.time()
        char_mapping = _build_char_mapping(list(effective_specs.keys()), all_char_names)
        logger.info(
            f"Char mapping (SequenceMatcher) computed in {time.time()-t0:.3f}s: "
            f"{len(char_mapping)}/{len(effective_specs)} mapped"
        )

    # Шаг 3: для каждой модели — O(1) lookup
    matches = []
    for eq_data in candidates_data:
        eq_id = eq_data["model_id"]
        eq_specs_list = specs_by_id.get(eq_id, [])
        eq_specs_dict: Dict[str, EquipmentSpec] = {s.char_name: s for s in eq_specs_list}
        eq_canonical_dict: Dict[str, EquipmentSpec] = {
            s.canonical_name: s for s in eq_specs_list if s.canonical_name
        }

        result = _match_one_model(
            effective_specs, eq_specs_dict, char_mapping, allow_lower,
            canonical_map=canonical_map,
            eq_canonical_dict=eq_canonical_dict,
        )
        specs_display = result.pop("matched_values", {})

        matches.append({
            **eq_data,
            "match_percentage": result["match_percentage"],
            "matched_specs": result["matched_specs"],
            "unmapped_specs": result["unmapped_specs"],
            "missing_specs": result["missing_specs"],
            "different_specs": result["different_specs"],
            "specifications": specs_display,
            "attributes": specs_display,
        })

    return matches


# ════════════════════════════════════════════════════════════════════════════
# Legacy: calculate_match_percentage_fuzzy (для совместимости)
# ════════════════════════════════════════════════════════════════════════════


def calculate_match_percentage_fuzzy(
    required_specs: Dict[str, Any],
    eq_specs: List[EquipmentSpec],
    allow_lower: bool = False,
) -> Dict[str, Any]:
    """Legacy: per-model fuzzy match. В продакшне используется _run_matching_sync."""
    if not required_specs:
        return {
            "match_percentage": 100.0,
            "matched_specs": [], "unmapped_specs": [], "missing_specs": [],
            "different_specs": {}, "matched_values": {},
        }
    all_char_names = {s.char_name for s in eq_specs}
    char_mapping = _build_char_mapping(list(required_specs.keys()), all_char_names)
    eq_specs_dict = {s.char_name: s for s in eq_specs}
    return _match_one_model(required_specs, eq_specs_dict, char_mapping, allow_lower)


# ════════════════════════════════════════════════════════════════════════════
# Категоризация результатов
# ════════════════════════════════════════════════════════════════════════════


def categorize_matches(
    matches: List[Dict[str, Any]], threshold: int = 70
) -> Dict[str, List[Dict[str, Any]]]:
    """Группировка результатов по категориям: ideal / partial / not_matched."""
    ideal = []
    partial = []
    not_matched = []

    for match in matches:
        percentage = match["match_percentage"]
        if percentage == 100.0:
            ideal.append(match)
        elif percentage >= threshold:
            partial.append(match)
        else:
            not_matched.append(match)

    ideal.sort(key=lambda x: x["model_name"])
    partial.sort(key=lambda x: x["match_percentage"], reverse=True)
    not_matched.sort(key=lambda x: x["match_percentage"], reverse=True)

    logger.info(
        f"Categorized: {len(ideal)} ideal, {len(partial)} partial, {len(not_matched)} not matched"
    )

    return {"ideal": ideal, "partial": partial, "not_matched": not_matched}


# ════════════════════════════════════════════════════════════════════════════
# Legacy: дедупликация (оставлена для совместимости с тестами)
# ════════════════════════════════════════════════════════════════════════════


def _parse_version_priority(source_file: str) -> float:
    if not source_file:
        return 0

    priority = 0.0

    m = re.search(r'finalUPDv\.(\d+)\.(\d+)', source_file)
    if m:
        priority = 1000 + int(m.group(2))
    elif 'finalUPD' in source_file:
        priority = 1000
    else:
        m = re.search(r'v(\d+)(?:\.(\d+))?', source_file)
        if m:
            priority = int(m.group(1))

    if '_new' in source_file:
        priority += 0.5

    return priority


def deduplicate_models(models) -> list:
    """Legacy: дедупликация списка моделей по model_name (для тестов)."""
    def _get_specs(m):
        return getattr(m, 'attributes', None) or getattr(m, 'specifications', None) or {}

    def _get_source(m):
        return getattr(m, 'source_filename', None) or getattr(m, 'source_file', None) or ""

    non_empty = [m for m in models if _get_specs(m)]
    filtered_count = len(models) - len(non_empty)
    if filtered_count:
        logger.info(f"Filtered out {filtered_count} models with empty specs")

    groups: dict = defaultdict(list)
    for model in non_empty:
        groups[model.model_name].append(model)

    result = []
    for name, group in groups.items():
        if len(group) == 1:
            result.append(group[0])
        else:
            best = max(
                group,
                key=lambda m: (
                    _parse_version_priority(_get_source(m)),
                    len(_get_specs(m)),
                ),
            )
            result.append(best)

    logger.info(f"Deduplicated: {len(models)} → {len(result)} models")
    return result


# Legacy alias for backward compat
def calculate_match_percentage(
    required_specs: Dict[str, Any],
    model_attrs: Dict[str, Any],
    allow_lower: bool = False,
) -> Dict[str, Any]:
    """
    Legacy: вычисление % совпадения через прямое сопоставление ключей (JSONB-стиль).
    Используется в существующих тестах. В продакшне — calculate_match_percentage_fuzzy.
    """
    if not required_specs:
        return {
            "match_percentage": 100.0,
            "matched_specs": [],
            "unmapped_specs": [],
            "missing_specs": [],
            "different_specs": {},
        }

    total_specs = len(required_specs)
    matched_count = 0
    matched_specs = []
    unmapped_specs = []
    different_specs = {}

    for key, required_value in required_specs.items():
        model_value = model_attrs.get(key)

        if model_value is None:
            unmapped_specs.append(key)
            continue

        if _compare_spec_values_legacy(required_value, model_value, key, allow_lower):
            matched_count += 1
            matched_specs.append(key)
        else:
            different_specs[key] = (required_value, model_value)

    match_percentage = (matched_count / total_specs) * 100.0

    return {
        "match_percentage": round(match_percentage, 2),
        "matched_specs": matched_specs,
        "unmapped_specs": unmapped_specs,
        "missing_specs": list(unmapped_specs),  # separate list, same semantics in legacy path
        "different_specs": different_specs,
    }


def _compare_spec_values_legacy(
    required_value: Any,
    model_value: Any,
    key: str,
    allow_lower: bool = False,
) -> bool:
    """Legacy сравнение для JSONB-стиля (используется в тестах)."""
    if model_value is None:
        return False
    if isinstance(required_value, bool):
        return bool(model_value) == required_value

    req_num, op = extract_number_with_operator(required_value)
    model_num = extract_number(model_value)

    if req_num is not None and model_num is not None:
        return _apply_operator(req_num, model_num, op, allow_lower)

    if isinstance(required_value, str) and isinstance(model_value, str):
        return compare_text_values(required_value, model_value)

    return required_value == model_value


# Backward compat alias — used by existing tests
compare_spec_values = _compare_spec_values_legacy


# ════════════════════════════════════════════════════════════════════════════
# Основная функция поиска и сопоставления
# ════════════════════════════════════════════════════════════════════════════


async def find_matching_models(requirements: Dict[str, Any]) -> Dict[str, Any]:
    """
    Поиск и сопоставление оборудования по требованиям из ТЗ.

    Args:
        requirements: структура от table_parser / inline_parser:
        {
            "items": [
                {
                    "model_name": str | null,
                    "category": str | null,
                    "required_specs": { original_char_name: value, ... }
                }
            ]
        }

    Стратегия поиска (fallback):
    1. Если указано model_name → поиск по названию
    2. Если указано category → поиск по категории (+ подкатегории)
    3. category=null → предупреждение, пропуск
    """
    items = requirements.get("items", [])
    if not items:
        logger.warning("No items in requirements")
        return {
            "results": [],
            "summary": {
                "total_requirements": 0,
                "total_models_found": 0,
                "ideal_matches": 0,
                "partial_matches": 0,
            },
        }

    results = []
    total_models_found = 0
    ideal_matches = 0
    partial_matches = 0

    threshold = settings.match_threshold
    allow_lower = settings.allow_lower_values

    logger.info(f"Starting matching: threshold={threshold}%, allow_lower={allow_lower}")

    for idx, item in enumerate(items, 1):
        model_name = item.get("model_name")
        category = item.get("category")
        required_specs = item.get("required_specs", {})

        logger.info(
            f"[Req {idx}/{len(items)}] model_name={model_name}, "
            f"category={category}, specs={len(required_specs)}"
        )

        # ── Поиск кандидатов ──
        candidates = []
        search_start = time.time()

        if model_name:
            candidates = list(await get_equipment_by_name(model_name))
            logger.info(f"Found {len(candidates)} by name in {time.time()-search_start:.3f}s")

        elif category:
            candidates = list(await get_equipment_by_category(category))
            initial_count = len(candidates)

            if category in CATEGORY_SUBCATEGORIES:
                for sub in CATEGORY_SUBCATEGORIES[category]:
                    sub_models = await get_equipment_by_category(sub)
                    candidates.extend(sub_models)
                logger.info(
                    f"Found {len(candidates)} (base {initial_count} + subcategories "
                    f"{len(candidates)-initial_count}) in {time.time()-search_start:.3f}s"
                )
            else:
                logger.info(f"Found {len(candidates)} in {time.time()-search_start:.3f}s")

        else:
            logger.warning(f"[Req {idx}] category=None and model_name=None — skipping")
            results.append({
                "requirement": item,
                "matches": {"ideal": [], "partial": [], "not_matched": []},
                "category_not_detected": True,
            })
            continue

        # ── Загружаем EAV спецификации для всех кандидатов одним запросом ──
        candidate_ids = [eq.id for eq in candidates]
        specs_by_id = await get_specs_by_equipment_ids(candidate_ids)
        logger.info(
            f"Loaded specs for {len(specs_by_id)} candidates "
            f"(total spec rows: {sum(len(v) for v in specs_by_id.values())})"
        )

        # ── Сопоставление: precomputed char_mapping + executor (не блокирует event loop) ──
        candidates_data = [
            {
                "model_id": eq.id,
                "model_name": eq.model_name,
                "category": eq.category,
                "version": eq.version,
                "source_filename": eq.source_filename,
            }
            for eq in candidates
        ]

        # Build all_char_names here so the async LLM call can use them
        all_char_names_for_llm: Set[str] = set()
        for specs_list in specs_by_id.values():
            for spec in specs_list:
                all_char_names_for_llm.add(spec.char_name)

        effective_specs_keys = [k for k in required_specs if k != "__canonical__"]
        char_mapping = await _build_char_mapping_llm(
            effective_specs_keys, all_char_names_for_llm
        )

        loop = asyncio.get_running_loop()
        match_start = time.time()
        matches = await loop.run_in_executor(
            None,
            functools.partial(
                _run_matching_sync,
                required_specs,
                candidates_data,
                specs_by_id,
                allow_lower,
                precomputed_char_mapping=char_mapping,
            ),
        )
        logger.info(f"Matching {len(candidates)} candidates done in {time.time()-match_start:.3f}s")

        # ── Категоризация ──
        categorized = categorize_matches(matches, threshold)
        results.append({"requirement": item, "matches": categorized})

        total_models_found += len(matches)
        ideal_matches += len(categorized["ideal"])
        partial_matches += len(categorized["partial"])

    summary = {
        "total_requirements": len(items),
        "total_models_found": total_models_found,
        "ideal_matches": ideal_matches,
        "partial_matches": partial_matches,
    }

    logger.info(f"Matching completed: {summary}")
    return {"results": results, "summary": summary}
