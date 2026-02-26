"""
Unit тесты для функций extract_number() и extract_number_with_operator() из services.matcher.

Покрывает все поддерживаемые форматы:
- Простые числа
- Строки с единицами измерения
- Дробные числа (точка и запятая)
- Диапазоны
- Умножение
- Префиксы
- Операторы сравнения (≥, ≤, >, <, =, !=)
- Edge cases
"""

import pytest
import sys
from pathlib import Path

# Добавляем корневую директорию проекта в PYTHONPATH
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from services.matcher import (
    compare_spec_values,
    compare_values_eav,
    extract_number,
    extract_number_with_operator,
    _apply_operator,
)


class TestSimpleNumbers:
    """Тесты для простых числовых значений."""

    def test_integer(self):
        assert extract_number(24) == 24.0

    def test_float(self):
        assert extract_number(200.5) == 200.5

    def test_negative(self):
        assert extract_number(-40) == -40.0

    def test_zero(self):
        assert extract_number(0) == 0.0


class TestStringsWithUnits:
    """Тесты для строк с единицами измерения."""

    def test_ports(self):
        assert extract_number("24 порта") == 24.0

    def test_watts(self):
        assert extract_number("200 Вт") == 200.0

    def test_gigabytes(self):
        # extract_number converts to MB base unit: 2 ГБ = 2*1024 = 2048 МБ
        assert extract_number("2 ГБ") == 2048.0

    def test_temperature(self):
        assert extract_number("-40°C") == -40.0

    def test_mixed_case(self):
        assert extract_number("100 МГц") == 100.0


class TestFractionalNumbers:
    """Тесты для дробных чисел."""

    def test_dot_separator(self):
        # extract_number converts to Mbps base unit: 1.5 Гбит/с = 1.5*1000 = 1500 Мбит/с
        assert extract_number("1.5 Гбит/с") == 1500.0

    def test_comma_separator(self):
        # extract_number converts to MB base unit: 2.5 ГБ = 2.5*1024 = 2560 МБ
        assert extract_number("2,5 ГБ") == 2560.0

    def test_multiple_dots(self):
        # Ожидаем первое число
        assert extract_number("1.2.3.4") == 1.2


class TestRanges:
    """Тесты для диапазонов (берем максимум)."""

    def test_simple_range(self):
        assert extract_number("10-20") == 20.0

    def test_range_with_words(self):
        assert extract_number("от 100 до 200") == 200.0

    def test_range_reversed(self):
        # 200-100 все равно берем максимум
        assert extract_number("200-100") == 200.0

    def test_range_with_units(self):
        assert extract_number("10-20 портов") == 20.0


class TestMultiplication:
    """Тесты для умножения."""

    def test_simple_multiplication(self):
        assert extract_number("2x4") == 8.0

    def test_multiplication_with_x_symbol(self):
        assert extract_number("4×8") == 32.0

    def test_blocks_pattern(self):
        assert extract_number("4 блока по 8") == 32.0

    def test_blocks_with_units(self):
        assert extract_number("2 блока по 10 портов") == 20.0


class TestPrefixes:
    """Тесты для префиксов."""

    def test_prefix_up_to(self):
        assert extract_number("до 1000") == 1000.0

    def test_prefix_minimum(self):
        assert extract_number("не менее 500") == 500.0

    def test_prefix_minimum_short(self):
        assert extract_number("минимум 100") == 100.0

    def test_prefix_maximum(self):
        assert extract_number("максимум 250") == 250.0


class TestOperatorsInExtractNumber:
    """Тесты: extract_number корректно извлекает число даже при наличии оператора."""

    def test_ge_unicode(self):
        assert extract_number("≥ 24") == 24.0

    def test_le_unicode(self):
        assert extract_number("≤ 100") == 100.0

    def test_ge_ascii(self):
        assert extract_number(">=24") == 24.0

    def test_le_ascii(self):
        assert extract_number("<=100") == 100.0

    def test_gt(self):
        assert extract_number("> 5") == 5.0

    def test_lt(self):
        assert extract_number("< 50") == 50.0

    def test_eq(self):
        assert extract_number("= 10") == 10.0


class TestEdgeCases:
    """Тесты для граничных случаев."""

    def test_none_value(self):
        assert extract_number(None) is None

    def test_empty_string(self):
        assert extract_number("") is None

    def test_no_numbers(self):
        assert extract_number("нет данных") is None

    def test_only_text(self):
        assert extract_number("неопределено") is None

    def test_boolean_true(self):
        assert extract_number(True) is None

    def test_boolean_false(self):
        assert extract_number(False) is None


class TestExtractNumberWithOperator:
    """Тесты для extract_number_with_operator()."""

    def test_ge_unicode(self):
        num, op = extract_number_with_operator("≥ 24")
        assert num == 24.0 and op == ">="

    def test_le_unicode(self):
        num, op = extract_number_with_operator("≤ 100")
        assert num == 100.0 and op == "<="

    def test_ge_ascii(self):
        num, op = extract_number_with_operator(">=24")
        assert num == 24.0 and op == ">="

    def test_le_ascii(self):
        num, op = extract_number_with_operator("<=100")
        assert num == 100.0 and op == "<="

    def test_gt(self):
        num, op = extract_number_with_operator("> 5")
        assert num == 5.0 and op == ">"

    def test_lt(self):
        num, op = extract_number_with_operator("< 50")
        assert num == 50.0 and op == "<"

    def test_eq(self):
        num, op = extract_number_with_operator("= 10")
        assert num == 10.0 and op == "="

    def test_ne_unicode(self):
        num, op = extract_number_with_operator("≠ 0")
        assert num == 0.0 and op == "!="

    def test_ne_ascii(self):
        num, op = extract_number_with_operator("!= 0")
        assert num == 0.0 and op == "!="

    def test_plain_string_number(self):
        num, op = extract_number_with_operator("24")
        assert num == 24.0 and op == ">="

    def test_plain_int(self):
        num, op = extract_number_with_operator(24)
        assert num == 24.0 and op == ">="

    def test_none(self):
        num, op = extract_number_with_operator(None)
        assert num is None and op == ">="

    def test_bool(self):
        num, op = extract_number_with_operator(True)
        assert num is None

    def test_text_ne_menee(self):
        num, op = extract_number_with_operator("не менее 500")
        assert num == 500.0 and op == ">="

    def test_text_ne_bolee(self):
        num, op = extract_number_with_operator("не более 100")
        assert num == 100.0 and op == "<="

    def test_text_do(self):
        num, op = extract_number_with_operator("до 1000")
        assert num == 1000.0 and op == "<="

    def test_text_minimum(self):
        num, op = extract_number_with_operator("минимум 50")
        assert num == 50.0 and op == ">="

    def test_text_maximum(self):
        num, op = extract_number_with_operator("максимум 200")
        assert num == 200.0 and op == "<="


class TestCompareSpecValues:
    """Интеграционные тесты для compare_spec_values с числовыми значениями."""

    def test_equal_integers(self):
        result = compare_spec_values(24, 24, "ports", allow_lower=False)
        assert result is True

    def test_model_greater(self):
        result = compare_spec_values(24, 30, "ports", allow_lower=False)
        assert result is True

    def test_model_lower(self):
        result = compare_spec_values(24, 20, "ports", allow_lower=False)
        assert result is False

    def test_strings_with_numbers_equal(self):
        result = compare_spec_values("24 порта", "24", "ports", allow_lower=False)
        assert result is True

    def test_strings_with_numbers_greater(self):
        result = compare_spec_values("24 порта", "30 портов", "ports", allow_lower=False)
        assert result is True

    def test_allow_lower_within_threshold(self):
        # 190 >= 200 * 0.95 (190) - должно пройти
        result = compare_spec_values(200, 190, "power", allow_lower=True)
        assert result is True

    def test_allow_lower_below_threshold(self):
        # 180 < 200 * 0.95 (190) - не должно пройти
        result = compare_spec_values(200, 180, "power", allow_lower=True)
        assert result is False

    def test_range_extraction(self):
        result = compare_spec_values("10-20", 25, "range", allow_lower=False)
        assert result is True  # model (25) >= required max (20)

    def test_multiplication_extraction(self):
        result = compare_spec_values("2x4", 10, "calc", allow_lower=False)
        assert result is True  # model (10) >= required (8)

    def test_le_operator_model_below(self):
        # ≤ 100: модель с 80 → True
        assert compare_spec_values("<=100", 80, "power") is True

    def test_le_operator_model_above(self):
        # ≤ 100: модель с 120 → False
        assert compare_spec_values("<=100", 120, "power") is False

    def test_ge_operator(self):
        assert compare_spec_values(">=24", 24, "ports") is True
        assert compare_spec_values(">=24", 12, "ports") is False


class TestBugFixes:
    """Regression tests for confirmed bugs (see bug report)."""

    # Bug 1: import_csv _extract_spec_value must store max of range as value_num.
    # Tested here at the extract_number level (same logic, same regex).
    def test_bug1_range_max_extracted(self):
        # "10-20 Гбит/с" should yield 20, not 10
        assert extract_number("10-20") == 20.0

    def test_bug1_range_max_asymmetric(self):
        # Unit multiplier is applied: 100 Гбит/с = 100_000 Мбит/с (base unit)
        assert extract_number("5-100 Гбит/с") == 100_000.0

    # Bug 2: compound DB value ">=32 и <=64" with single TZ requirement ">=50"
    def test_bug2_compound_db_single_tz_pass(self):
        # Device range [32..64], TZ requires >=50 → 64 >= 50 → True
        assert compare_values_eav(">=50", ">=32 и <=64", 32) is True

    def test_bug2_compound_db_single_tz_fail(self):
        # Device range [32..40], TZ requires >=50 → 40 < 50 → False
        assert compare_values_eav(">=50", ">=32 и <=40", 32) is False

    def test_bug2_compound_db_le_tz(self):
        # Device range [32..64], TZ requires <=35 → lower bound 32 <= 35 → True
        assert compare_values_eav("<=35", ">=32 и <=64", 32) is True

    def test_bug2_compound_db_le_tz_fail(self):
        # Device range [40..64], TZ requires <=35 → lower bound 40 > 35 → False
        assert compare_values_eav("<=35", ">=40 и <=64", 40) is False

    # Bug 4: > and < operators must respect allow_lower tolerance
    def test_bug4_gt_allow_lower_within_tolerance(self):
        # model=30.5, req=32, op=">", allow_lower=True → 30.5 > 32*0.95=30.4 → True
        assert _apply_operator(32, 30.5, ">", allow_lower=True) is True

    def test_bug4_gt_no_allow_lower(self):
        # Without allow_lower: 30.5 > 32 → False
        assert _apply_operator(32, 30.5, ">", allow_lower=False) is False

    def test_bug4_lt_allow_lower_within_tolerance(self):
        # model=105, req=100, op="<", allow_lower=True → 105 < 100*1.05=105 → False (boundary)
        assert _apply_operator(100, 104, "<", allow_lower=True) is True

    def test_bug4_lt_no_allow_lower(self):
        # Without allow_lower: 104 < 100 → False
        assert _apply_operator(100, 104, "<", allow_lower=False) is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
