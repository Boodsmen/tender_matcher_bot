"""Shared fixtures for tests."""

from unittest.mock import MagicMock


def make_equipment(
    model_name: str,
    source_filename: str = "Category_Switch_test.xlsx",
    version: str | None = None,
    attributes: dict | None = None,
    model_id: int = 1,
    category: str = "Коммутаторы",
) -> MagicMock:
    """Create a mock Equipment object for testing (avoids DB dependency)."""
    attrs = attributes if attributes is not None else {}
    m = MagicMock()
    m.id = model_id
    m.model_name = model_name
    m.source_filename = source_filename
    m.version = version
    m.attributes = attrs
    m.category = category
    # Backward-compat aliases used by legacy tests and deduplicate_models
    m.specifications = attrs
    m.source_file = source_filename
    m.raw_specifications = {}
    return m


def make_model(
    model_name: str,
    source_file: str = "test.csv",
    specifications: dict | None = None,
    raw_specifications: dict | None = None,
    model_id: int = 1,
    category: str = "Коммутаторы",
) -> MagicMock:
    """Legacy alias — prefer make_equipment() for new tests."""
    return make_equipment(
        model_name=model_name,
        source_filename=source_file,
        version=None,
        attributes=specifications,
        model_id=model_id,
        category=category,
    )
