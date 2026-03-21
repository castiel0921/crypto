"""llm_analyst_reports, llm_prompt_templates, pipeline_run_log

Revision ID: 003
Revises: 002
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '003'
down_revision = '002'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'llm_analyst_reports',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('scan_batch_id', sa.String(36), nullable=False),
        sa.Column('report_time', sa.DateTime, server_default=sa.func.now()),
        sa.Column('btc_regime', sa.String(20), nullable=True),
        sa.Column('btc_narrative', sa.Text, nullable=True),
        sa.Column('top_long', sa.JSON, nullable=True),
        sa.Column('top_short', sa.JSON, nullable=True),
        sa.Column('warnings', sa.JSON, nullable=True),
        sa.Column('market_summary', sa.Text, nullable=True),
        sa.Column('candidate_count', sa.Integer, nullable=True),
        sa.Column('prompt_version', sa.String(10), nullable=True),
    )
    op.create_index('ix_lar_batch', 'llm_analyst_reports', ['scan_batch_id'])

    op.create_table(
        'llm_prompt_templates',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('module', sa.String(20), nullable=True),
        sa.Column('version', sa.String(10), nullable=True),
        sa.Column('content', sa.Text, nullable=False),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now()),
        sa.Column('notes', sa.Text, nullable=True),
        sa.UniqueConstraint('module', 'version', name='uq_prompt_mv'),
    )

    op.create_table(
        'pipeline_run_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('job_id', sa.String(36), unique=True, nullable=False),
        sa.Column('batch_id', sa.String(36), nullable=True),
        sa.Column('interval', sa.String(10), nullable=True),
        sa.Column('triggered_at', sa.DateTime, nullable=False),
        sa.Column('finished_at', sa.DateTime, nullable=True),
        sa.Column('stage', sa.String(30), nullable=True),
        sa.Column('status', sa.String(20), nullable=True),
        sa.Column('symbols_total', sa.Integer, nullable=True),
        sa.Column('symbols_fetched', sa.Integer, nullable=True),
        sa.Column('symbols_scannable', sa.Integer, nullable=True),
        sa.Column('symbols_skipped', sa.Integer, nullable=True),
        sa.Column('patterns_found', sa.Integer, nullable=True),
        sa.Column('llm_reviewed', sa.Integer, nullable=True),
        sa.Column('llm_success', sa.Integer, nullable=True),
        sa.Column('llm_timeout', sa.Integer, nullable=True),
        sa.Column('error_stage', sa.String(30), nullable=True),
        sa.Column('error_message', sa.Text, nullable=True),
        sa.Column('failed_symbols', sa.JSON, nullable=True),
        sa.Column('duration_sec', sa.Float, nullable=True),
    )
    op.create_index('ix_prl_triggered', 'pipeline_run_log', ['triggered_at'])


def downgrade() -> None:
    op.drop_index('ix_prl_triggered', 'pipeline_run_log')
    op.drop_table('pipeline_run_log')
    op.drop_table('llm_prompt_templates')
    op.drop_index('ix_lar_batch', 'llm_analyst_reports')
    op.drop_table('llm_analyst_reports')
