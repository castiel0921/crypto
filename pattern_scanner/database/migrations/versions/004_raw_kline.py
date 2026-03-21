"""raw_kline_store

Revision ID: 004
Revises: 003
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '004'
down_revision = '003'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'raw_kline_store',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.String(20), nullable=False),
        sa.Column('interval', sa.String(10), nullable=False),
        sa.Column('open_time', sa.DateTime, nullable=False),
        sa.Column('open', sa.Float, nullable=False),
        sa.Column('high', sa.Float, nullable=False),
        sa.Column('low', sa.Float, nullable=False),
        sa.Column('close', sa.Float, nullable=False),
        sa.Column('volume', sa.Float, nullable=False),
        sa.UniqueConstraint('symbol', 'interval', 'open_time', name='uq_raw_kline'),
    )
    op.create_index('ix_raw_sym_iv_time', 'raw_kline_store', ['symbol', 'interval', 'open_time'])


def downgrade() -> None:
    op.drop_index('ix_raw_sym_iv_time', 'raw_kline_store')
    op.drop_table('raw_kline_store')
