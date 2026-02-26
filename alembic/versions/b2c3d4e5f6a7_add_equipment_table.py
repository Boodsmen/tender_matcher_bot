"""Add equipment table (replace models)

Revision ID: b2c3d4e5f6a7
Revises: 5967ff94d7bc
Create Date: 2026-02-18 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, None] = '5967ff94d7bc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop old models table (CASCADE removes dependent indexes)
    op.drop_index('idx_specifications_gin', table_name='models', postgresql_using='gin')
    op.drop_index('idx_source_file', table_name='models')
    op.drop_index('idx_model_name', table_name='models')
    op.drop_index('idx_category', table_name='models')
    op.drop_table('models')

    # Create new equipment table
    op.create_table(
        'equipment',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('category', sa.String(length=255), nullable=False),
        sa.Column('model_name', sa.String(length=255), nullable=False),
        sa.Column('version', sa.String(length=100), nullable=True),
        sa.Column('source_filename', sa.String(length=500), nullable=False),
        sa.Column('attributes', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_equipment_category', 'equipment', ['category'], unique=False)
    op.create_index('idx_equipment_model_name', 'equipment', ['model_name'], unique=False)
    op.create_index(
        'idx_attributes_gin', 'equipment', ['attributes'],
        unique=False, postgresql_using='gin',
    )


def downgrade() -> None:
    op.drop_index('idx_attributes_gin', table_name='equipment', postgresql_using='gin')
    op.drop_index('idx_equipment_model_name', table_name='equipment')
    op.drop_index('idx_equipment_category', table_name='equipment')
    op.drop_table('equipment')

    # Recreate old models table
    op.create_table(
        'models',
        sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
        sa.Column('model_name', sa.String(length=255), nullable=False),
        sa.Column('category', sa.String(length=255), nullable=True),
        sa.Column('source_file', sa.String(length=100), nullable=False),
        sa.Column('specifications', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column('raw_specifications', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('created_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.Column('updated_at', sa.DateTime(), server_default=sa.text('now()'), nullable=False),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index('idx_category', 'models', ['category'], unique=False)
    op.create_index('idx_model_name', 'models', ['model_name'], unique=False)
    op.create_index('idx_source_file', 'models', ['source_file'], unique=False)
    op.create_index(
        'idx_specifications_gin', 'models', ['specifications'],
        unique=False, postgresql_using='gin',
    )
