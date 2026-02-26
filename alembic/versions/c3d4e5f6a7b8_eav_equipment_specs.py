"""EAV: add equipment_specs, drop attributes from equipment

Revision ID: c3d4e5f6a7b8
Revises: b2c3d4e5f6a7
Create Date: 2026-02-18 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'c3d4e5f6a7b8'
down_revision: Union[str, None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop GIN index and attributes column from equipment
    op.drop_index('idx_attributes_gin', table_name='equipment', postgresql_using='gin')
    op.drop_column('equipment', 'attributes')

    # Create equipment_specs table (EAV)
    op.create_table(
        'equipment_specs',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('equipment_id', sa.Integer(), nullable=False),
        sa.Column('char_name', sa.Text(), nullable=False),
        sa.Column('value_text', sa.Text(), nullable=True),
        sa.Column('value_num', sa.Float(), nullable=True),
        sa.ForeignKeyConstraint(
            ['equipment_id'], ['equipment.id'],
            ondelete='CASCADE',
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        'idx_equipment_specs_equipment_id',
        'equipment_specs',
        ['equipment_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('idx_equipment_specs_equipment_id', table_name='equipment_specs')
    op.drop_table('equipment_specs')

    op.add_column(
        'equipment',
        sa.Column(
            'attributes',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default='{}',
        ),
    )
    op.create_index(
        'idx_attributes_gin', 'equipment', ['attributes'],
        unique=False, postgresql_using='gin',
    )
