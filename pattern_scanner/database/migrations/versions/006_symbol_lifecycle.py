"""symbol_universe: lifecycle fields already in migration 001; this ensures pattern_name column

Revision ID: 006
Revises: 005
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '006'
down_revision = '005'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # pattern_name was added in 002 directly; this migration is reserved
    # for any future symbol lifecycle additions
    pass


def downgrade() -> None:
    pass
