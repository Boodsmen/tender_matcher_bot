"""
Import equipment data from XLSX / CSV files in data/csv/ into the equipment table.

EAV schema: each characteristic is stored as a row in equipment_specs
with char_name = original column name (no normalization).

File naming convention:
  Category_Switch_*.xlsx  →  category="Коммутаторы"
  Category_Router_*.xlsx  →  category="Маршрутизаторы"
  *.csv                   →  category determined by CATEGORY_MAPPING (filename prefix)

Usage:
  python scripts/import_csv.py
"""

import asyncio
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Resolve project root
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from utils.logger import logger

DATA_DIR = os.path.join(PROJECT_ROOT, "data", "csv")

# ──────────────────────────── Canonical name mapping ───────────

_NORMALIZATION_MAP_PATH = os.path.join(PROJECT_ROOT, "data", "normalization_map.json")

def _build_reverse_map() -> Dict[str, str]:
    """Build {russian_name_lower: canonical_key} from normalization_map.json."""
    try:
        with open(_NORMALIZATION_MAP_PATH, encoding="utf-8") as f:
            data = json.load(f)
        reverse: Dict[str, str] = {}
        for canonical_key, synonyms in data.get("canonical_keys", {}).items():
            for syn in synonyms:
                reverse[syn.lower().strip()] = canonical_key
        return reverse
    except Exception as e:
        logger.warning(f"Could not load normalization_map.json: {e}")
        return {}

_REVERSE_MAP: Dict[str, str] = _build_reverse_map()


def _get_canonical_name(char_name: str) -> Optional[str]:
    return _REVERSE_MAP.get(char_name.lower().strip())

# ──────────────────────────── Category mapping ─────────────────

# Fallback for CSV files without Category_ prefix
CATEGORY_MAPPING = {
    "MES": "Коммутаторы",
    "ESR": "Маршрутизаторы",
    "ISS": "Коммутаторы",
    "Fastpath": "Коммутаторы",
    "ME": "Коммутаторы",
    "ROS4": "Коммутаторы",
    "ROS6": "Коммутаторы",
    "1805": "Маршрутизаторы",
    "T-TTv2": "Маршрутизаторы",
}

# Column names that typically hold the model name
MODEL_NAME_CANDIDATES = [
    "model_name",
    "Model",
    "Модель",
    "Наименование",
    "Наименование модели",
    "Название модели",
    "Unnamed: 0",
]

# Columns to skip when building specs
SKIP_COLUMNS = {"model_name", "category", "Категория", "Тип коммутатора", "Тип устройства"}

# EAV format column names (case-insensitive detection)
_EAV_MODEL_COL = "модель"
_EAV_CHAR_COL = "характеристика"
_EAV_VAL_COL = "значение"

# Null-like values to skip
_NULL_VALUES = {"", "-", "—", "н/д", "n/a", "nan", "none", "нет данных"}


# ──────────────────────────── Value extraction ──────────────────


def _extract_spec_value(value: Any) -> Tuple[Optional[str], Optional[float]]:
    """
    Extract (value_text, value_num) from a raw cell value.

    Returns:
        (value_text, value_num) — both may be None if value is empty/null.
        value_text is always a non-empty string if not None.
        value_num is float or None.
    """
    if value is None:
        return None, None
    if isinstance(value, float) and pd.isna(value):
        return None, None
    if isinstance(value, bool):
        value_text = "Да" if value else "Нет"
        return value_text, None

    value_str = str(value).strip()
    if value_str.lower() in _NULL_VALUES:
        return None, None

    # Extract numeric value
    value_num: Optional[float] = None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        value_num = float(value)
    else:
        # Sum: "24+4" → 28
        sum_match = re.match(r'^(\d+)\s*\+\s*(\d+)$', value_str.strip())
        if sum_match:
            value_num = float(int(sum_match.group(1)) + int(sum_match.group(2)))
        else:
            # Product: "24x4" / "24х4"
            mult_match = re.match(r'^(\d+)\s*[xхX×]\s*(\d+)$', value_str.strip())
            if mult_match:
                value_num = float(int(mult_match.group(1)) * int(mult_match.group(2)))
            else:
                # Range: "10-20" → use max (e.g. "10-20 Гбит/с" → 20)
                range_match = re.search(r'(\d+(?:\.\d+)?)\s*[-–]\s*(\d+(?:\.\d+)?)', value_str)
                if range_match:
                    value_num = max(float(range_match.group(1)), float(range_match.group(2)))
                else:
                    # First number in string
                    digits = re.findall(r'-?\d+\.?\d*', value_str)
                    if digits:
                        try:
                            value_num = float(digits[0])
                        except ValueError:
                            pass

    return value_str, value_num


# ──────────────────────────── Category / Version from filename ──


def extract_category_from_filename(filename: str) -> Optional[str]:
    """Determine category from Category_Switch / Category_Router prefix."""
    name = os.path.splitext(filename)[0]
    if "Category_Switch" in name:
        return "Коммутаторы"
    if "Category_Router" in name:
        return "Маршрутизаторы"
    # Fallback: model prefix mapping
    for prefix, cat in CATEGORY_MAPPING.items():
        if prefix.lower() in name.lower():
            return cat
    return None


def parse_version_from_filename(filename: str) -> Optional[str]:
    """Extract version string from filename."""
    name = os.path.splitext(filename)[0]

    if re.search(r'final', name, re.IGNORECASE):
        m = re.search(r'[_\s]v\.?(\d+)[.,](\d+)', name, re.IGNORECASE)
        if m:
            return f"finalUPD v{m.group(1)}.{m.group(2)}"
        m = re.search(r'[_\s]v\.?(\d+)', name, re.IGNORECASE)
        if m:
            return f"finalUPD v{m.group(1)}"
        return "finalUPD"

    m = re.search(r'_v(\d+)(?:[.,](\d+))?(?!\d)', name, re.IGNORECASE)
    if m:
        if m.group(2):
            return f"v{m.group(1)}.{m.group(2)}"
        return f"v{m.group(1)}"

    m = re.search(r'(\d{2}[.,]\d{2}(?:[.,]\d{4})?)', name)
    if m:
        return m.group(1)

    return None


# ──────────────────────────── Model name column detection ───────


def detect_model_name_column(columns: List[str]) -> Optional[str]:
    """Find which column holds the model name."""
    cols_lower = {c.lower().strip(): c for c in columns}
    for candidate in MODEL_NAME_CANDIDATES:
        if candidate.lower() in cols_lower:
            return cols_lower[candidate.lower()]
    return columns[0] if columns else None


# ──────────────────────────── File parsers ─────────────────────


def _load_dataframe(filepath: str, filename: str) -> Optional[pd.DataFrame]:
    """Load a CSV or XLSX file into a DataFrame."""
    ext = os.path.splitext(filename)[1].lower()
    try:
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(filepath, engine="openpyxl")
        else:
            try:
                df = pd.read_csv(filepath, encoding="utf-8")
            except UnicodeDecodeError:
                df = pd.read_csv(filepath, encoding="cp1251")
    except Exception as e:
        logger.error(f"Failed to read {filename}: {e}")
        return None

    if df.empty:
        logger.warning(f"Empty file: {filename}")
        return None

    df.columns = [str(c).strip() for c in df.columns]
    return df


def _is_eav_format(df: pd.DataFrame) -> bool:
    """Return True if the DataFrame has EAV columns: Модель, Характеристика, Значение."""
    cols_lower = {c.lower() for c in df.columns}
    return {_EAV_MODEL_COL, _EAV_CHAR_COL, _EAV_VAL_COL}.issubset(cols_lower)


def _parse_eav(
    df: pd.DataFrame,
    filename: str,
    category: str,
    version: Optional[str],
) -> List[Dict[str, Any]]:
    """Parse EAV format: rows are (Модель, Характеристика, Значение) triples."""
    col_map = {c.lower(): c for c in df.columns}
    model_col = col_map[_EAV_MODEL_COL]
    char_col = col_map[_EAV_CHAR_COL]
    val_col = col_map[_EAV_VAL_COL]

    # Group rows by model name
    model_specs: Dict[str, List[Tuple[str, Optional[str], Optional[float], Optional[str]]]] = {}
    seen_chars: Dict[str, set] = {}  # model_name -> set of char_names (dedup)

    for _, row in df.iterrows():
        model_name = row.get(model_col)
        if model_name is None or (isinstance(model_name, float) and pd.isna(model_name)):
            continue
        model_name = str(model_name).strip()
        if not model_name or model_name.lower() in ("nan", "none", ""):
            continue

        char_name = row.get(char_col)
        if char_name is None or (isinstance(char_name, float) and pd.isna(char_name)):
            continue
        char_name = str(char_name).strip()
        if not char_name:
            continue

        value_text, value_num = _extract_spec_value(row.get(val_col))
        if value_text is None:
            continue

        if model_name not in model_specs:
            model_specs[model_name] = []
            seen_chars[model_name] = set()

        # Keep first non-None value for duplicate characteristics
        if char_name not in seen_chars[model_name]:
            seen_chars[model_name].add(char_name)
            canonical = _get_canonical_name(char_name)
            model_specs[model_name].append((char_name, value_text, value_num, canonical))

    records = []
    for model_name, specs in model_specs.items():
        if not specs:
            continue
        records.append({
            "model_name": model_name,
            "category": category,
            "version": version,
            "source_filename": filename,
            "specs": specs,
        })
    return records


def parse_file(
    filepath: str,
    filename: str,
) -> List[Dict[str, Any]]:
    """Parse a single file and return a list of equipment dicts ready for DB insert."""
    df = _load_dataframe(filepath, filename)
    if df is None:
        return []

    category = extract_category_from_filename(filename)
    if not category:
        logger.warning(f"Cannot determine category for {filename} — skipping")
        return []

    version = parse_version_from_filename(filename)

    # EAV format: Модель | Характеристика | Значение
    if _is_eav_format(df):
        return _parse_eav(df, filename, category, version)

    # Wide format: each row = one model, columns = characteristics
    model_col = detect_model_name_column(list(df.columns))
    if model_col is None:
        logger.error(f"Cannot detect model_name column in {filename}")
        return []

    records: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        model_name = row.get(model_col)
        if model_name is None or (isinstance(model_name, float) and pd.isna(model_name)):
            continue
        model_name = str(model_name).strip()
        if not model_name or model_name.lower() in ("nan", "none", ""):
            continue

        specs: List[Tuple[str, Optional[str], Optional[float], Optional[str]]] = []
        seen_chars: set = set()
        row_dict = row.to_dict()

        for column, value in row_dict.items():
            if column == model_col or column in SKIP_COLUMNS:
                continue
            # Use original column name (no normalization)
            char_name = column
            value_text, value_num = _extract_spec_value(value)
            if value_text is not None and char_name not in seen_chars:
                seen_chars.add(char_name)
                canonical = _get_canonical_name(char_name)
                specs.append((char_name, value_text, value_num, canonical))

        if not specs:
            continue

        records.append({
            "model_name": model_name,
            "category": category,
            "version": version,
            "source_filename": filename,
            "specs": specs,
        })

    return records


# ──────────────────────────── Main import logic ─────────────────


async def import_all_files():
    """Import all XLSX/CSV files from data/csv/ into the equipment table."""
    from database.crud import (
        bulk_create_equipment_with_specs,
        delete_all_equipment,
        get_equipment_count,
    )

    # Collect supported files
    all_files = sorted(
        f for f in os.listdir(DATA_DIR)
        if f.lower().endswith((".xlsx", ".xls", ".csv"))
    )
    if not all_files:
        logger.error(f"No supported files found in {DATA_DIR}")
        return

    logger.info(f"Found {len(all_files)} files to import")

    # Clear existing equipment (specs deleted via CASCADE)
    deleted = await delete_all_equipment()
    if deleted:
        logger.info(f"Cleared {deleted} existing equipment records")

    total_imported = 0
    skipped = 0

    for i, filename in enumerate(all_files, 1):
        filepath = os.path.join(DATA_DIR, filename)
        try:
            records = parse_file(filepath, filename)
            if records:
                count = await bulk_create_equipment_with_specs(records)
                total_imported += count
                logger.info(f"[{i}/{len(all_files)}] {filename}: {count} records imported")
            else:
                skipped += 1
                logger.warning(f"[{i}/{len(all_files)}] {filename}: no records found or skipped")
        except Exception as e:
            logger.error(f"[{i}/{len(all_files)}] {filename}: ERROR — {e}", exc_info=True)
            skipped += 1

    total_in_db = await get_equipment_count()
    logger.info(
        f"\nImport complete: {total_imported} records imported from "
        f"{len(all_files) - skipped} files ({skipped} skipped). "
        f"Total in DB: {total_in_db}"
    )


def main():
    print("Starting equipment import...")
    asyncio.run(import_all_files())


if __name__ == "__main__":
    main()
