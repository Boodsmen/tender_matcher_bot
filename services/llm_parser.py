"""LLM-powered TZ parser using OpenAI API."""

import json
import os
import re
from typing import Any, Dict, List, Optional

from utils.logger import logger

# ──────────────────────────── Canonical vocabulary ──────────────

_NORMALIZATION_MAP_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "data", "normalization_map.json"
)


def _load_canonical_vocab() -> Dict[str, str]:
    """Load {canonical_key: first_synonym} for the LLM prompt."""
    try:
        with open(_NORMALIZATION_MAP_PATH, encoding="utf-8") as f:
            data = json.load(f)
        vocab: Dict[str, str] = {}
        for key, synonyms in data.get("canonical_keys", {}).items():
            if synonyms:
                vocab[key] = synonyms[0]
        return vocab
    except Exception as e:
        logger.warning(f"llm_parser: could not load normalization_map.json: {e}")
        return {}


CANONICAL_VOCAB: Dict[str, str] = _load_canonical_vocab()

# ──────────────────────────── Prompt ────────────────────────────

_SYSTEM_PROMPT_TEMPLATE = """Ты парсер технических заданий на сетевое оборудование (коммутаторы, маршрутизаторы).
Извлеки все позиции оборудования и их характеристики из текста ТЗ.

Для каждой характеристики:
1. Определи canonical_name из словаря, если подходит (используй ТОЧНЫЙ ключ):
{vocab_snippet}
2. Если в словаре нет подходящего — canonical_name = null.

Операторы: ">=", "<=", ">", "<", "=".
Для диапазонов (например "> 512 и <= 1024") используй operator_min + value_min + operator_max + value_max.

Отвечай ТОЛЬКО JSON, без пояснений:
{{
  "items": [
    {{
      "item_name": "Коммутатор уровня доступа",
      "category": "Коммутаторы",
      "model_name": null,
      "requirements": [
        {{"char_name": "Кол-во портов GE", "canonical_name": "ports_1g_8p8c", "operator": ">=", "value": 24, "unit": "шт"}},
        {{"char_name": "Объём RAM", "canonical_name": "ram", "operator_min": ">", "value_min": 512, "operator_max": "<=", "value_max": 1024, "unit": "МБ"}}
      ]
    }}
  ]
}}"""


def _build_vocab_snippet() -> str:
    """Build a compact vocab string for the prompt (max ~60 entries)."""
    lines = []
    for key, name in list(CANONICAL_VOCAB.items())[:60]:
        lines.append(f'  "{key}": "{name}"')
    return "{\n" + ",\n".join(lines) + "\n}"


# ──────────────────────────── Client ────────────────────────────

_client = None  # lazy init


def _get_client():
    global _client
    if _client is None:
        from openai import AsyncOpenAI
        from config import settings
        _client = AsyncOpenAI(api_key=settings.openai_api_key)
    return _client


# ──────────────────────────── Normalizer ────────────────────────


def _normalize_llm_output(data: dict) -> dict:
    """Convert LLM JSON format to internal requirements format."""
    items = []
    for item in data.get("items", []):
        required_specs: Dict[str, Any] = {}
        canonical_map: Dict[str, str] = {}

        for req in item.get("requirements", []):
            char_name = req.get("char_name", "").strip()
            if not char_name:
                continue
            canonical = req.get("canonical_name")

            if "value_min" in req and "value_max" in req:
                op_min = req.get("operator_min", ">")
                op_max = req.get("operator_max", "<=")
                required_specs[char_name] = [
                    f"{op_min}{req['value_min']}",
                    f"{op_max}{req['value_max']}",
                ]
            else:
                op = req.get("operator", ">=")
                val = req.get("value")
                if val is not None:
                    required_specs[char_name] = f"{op}{val}" if op not in ("=", "") else val

            if canonical:
                canonical_map[char_name] = canonical

        if canonical_map:
            required_specs["__canonical__"] = canonical_map

        items.append({
            "item_name": item.get("item_name"),
            "model_name": item.get("model_name"),
            "category": item.get("category"),
            "required_specs": required_specs,
        })

    return {"items": items}


# ──────────────────────────── Main API ──────────────────────────


async def parse_tz_with_llm(document_text: str) -> Optional[dict]:
    """
    Parse TZ document with LLM.

    Returns requirements dict (same format as table_parser) or None on failure.
    """
    try:
        client = _get_client()
    except Exception as e:
        logger.error(f"LLM client init failed: {e}")
        return None

    system_prompt = _SYSTEM_PROMPT_TEMPLATE.format(
        vocab_snippet=_build_vocab_snippet()
    )

    try:
        from config import settings
        resp = await client.chat.completions.create(
            model=settings.openai_model,
            max_tokens=8192,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": document_text[:20000]},
            ],
        )
        raw = resp.choices[0].message.content
        # Use raw_decode to find the first valid JSON object without greedy matching
        try:
            start_idx = raw.index('{')
            decoder = json.JSONDecoder()
            data, _ = decoder.raw_decode(raw, start_idx)
        except (ValueError, json.JSONDecodeError):
            logger.warning("LLM parser: no JSON found in response")
            return None
        result = _normalize_llm_output(data)
        if result.get("items"):
            logger.info(f"LLM parser: {len(result['items'])} items extracted")
            return result
        logger.warning("LLM parser: items list is empty")
        return None
    except Exception as e:
        logger.error(f"LLM parser failed: {e}")
        return None
