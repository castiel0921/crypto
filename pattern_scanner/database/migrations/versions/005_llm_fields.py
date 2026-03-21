"""pattern_scan_results: add llm_* fields

Revision ID: 005
Revises: 004
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '005'
down_revision = '004'
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table('pattern_scan_results') as batch_op:
        batch_op.add_column(sa.Column('llm_confidence', sa.String(10), nullable=True))
        batch_op.add_column(sa.Column('llm_risk', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('llm_enter_pool', sa.Boolean, nullable=True))
        batch_op.add_column(sa.Column('llm_reasoning', sa.Text, nullable=True))
        batch_op.add_column(sa.Column('llm_prompt_ver', sa.String(10), nullable=True))
        batch_op.add_column(sa.Column('llm_reviewed_at', sa.DateTime, nullable=True))
        batch_op.create_index('ix_psr_llm_conf', ['llm_confidence'])


def downgrade() -> None:
    with op.batch_alter_table('pattern_scan_results') as batch_op:
        batch_op.drop_index('ix_psr_llm_conf')
        batch_op.drop_column('llm_reviewed_at')
        batch_op.drop_column('llm_prompt_ver')
        batch_op.drop_column('llm_reasoning')
        batch_op.drop_column('llm_enter_pool')
        batch_op.drop_column('llm_risk')
        batch_op.drop_column('llm_confidence')
