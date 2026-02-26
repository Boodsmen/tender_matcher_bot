"""Add canonical_name to equipment_specs + indexes

Revision ID: d4e5f6a7b8c9
Revises: c3d4e5f6a7b8
Create Date: 2026-02-18 14:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd4e5f6a7b8c9'
down_revision: Union[str, None] = 'c3d4e5f6a7b8'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'equipment_specs',
        sa.Column('canonical_name', sa.Text(), nullable=True),
    )
    op.create_index('idx_specs_canonical_name', 'equipment_specs', ['canonical_name'])
    op.create_index(
        'idx_specs_value_num',
        'equipment_specs',
        ['value_num'],
        postgresql_where=sa.text("value_num IS NOT NULL"),
    )
    # pg_trgm extension for fallback fuzzy search on char_name
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    op.create_index(
        'idx_specs_char_name_trgm',
        'equipment_specs',
        ['char_name'],
        postgresql_using='gin',
        postgresql_ops={'char_name': 'gin_trgm_ops'},
    )


def downgrade() -> None:
    op.drop_index('idx_specs_char_name_trgm', table_name='equipment_specs')
    op.drop_index('idx_specs_value_num', table_name='equipment_specs')
    op.drop_index('idx_specs_canonical_name', table_name='equipment_specs')
    op.drop_column('equipment_specs', 'canonical_name')
