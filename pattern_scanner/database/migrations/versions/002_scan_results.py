"""pattern_scan_results, pattern_backtest_stats, pattern_combinations

Revision ID: 002
Revises: 001
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '002'
down_revision = '001'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'pattern_scan_results',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.String(20), nullable=False),
        sa.Column('timeframe', sa.String(10), nullable=False),
        sa.Column('bar_time', sa.DateTime, nullable=False),
        sa.Column('pattern_id', sa.String(10), nullable=True),
        sa.Column('pattern_name', sa.String(60), nullable=True),
        sa.Column('direction', sa.String(10), nullable=True),
        sa.Column('regime', sa.String(20), nullable=True),
        sa.Column('regime_score', sa.Float, nullable=True),
        sa.Column('total_score', sa.Float, nullable=True),
        sa.Column('confirm_score', sa.Float, nullable=True),
        sa.Column('exclude_penalty', sa.Float, nullable=True),
        sa.Column('trigger_met', sa.Boolean, nullable=True),
        sa.Column('trigger_type', sa.String(30), nullable=True),
        sa.Column('invalidated', sa.Boolean, default=False),
        sa.Column('invalidation_reason', sa.String(50), nullable=True),
        sa.Column('is_filter_hit', sa.Boolean, default=False),
        sa.Column('field_results', sa.JSON, nullable=True),
        sa.Column('raw_values', sa.JSON, nullable=True),
        sa.Column('scan_batch_id', sa.String(36), nullable=True),
        sa.Column('rule_version', sa.String(20), nullable=True),
        sa.Column('created_at', sa.DateTime, server_default=sa.func.now()),
        sa.UniqueConstraint('symbol', 'timeframe', 'bar_time', 'pattern_id', name='uq_scan'),
    )
    op.create_index('ix_psr_batch', 'pattern_scan_results', ['scan_batch_id'])
    op.create_index('ix_psr_sym_time', 'pattern_scan_results', ['symbol', 'bar_time'])
    op.create_index('ix_psr_score', 'pattern_scan_results', ['total_score'])

    op.create_table(
        'pattern_backtest_stats',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('pattern_id', sa.String(10), nullable=False),
        sa.Column('regime', sa.String(20), nullable=True),
        sa.Column('timeframe', sa.String(10), nullable=True),
        sa.Column('forward_bars', sa.Integer, default=10),
        sa.Column('trigger_only', sa.Boolean, default=True),
        sa.Column('sample_size', sa.Integer, nullable=True),
        sa.Column('win_rate', sa.Float, nullable=True),
        sa.Column('avg_return', sa.Float, nullable=True),
        sa.Column('avg_holding_bars', sa.Float, nullable=True),
        sa.Column('max_drawdown', sa.Float, nullable=True),
        sa.Column('sharpe_like', sa.Float, nullable=True),
        sa.Column('llm_high_conf_win_rate', sa.Float, nullable=True),
        sa.Column('stat_period_start', sa.DateTime, nullable=True),
        sa.Column('stat_period_end', sa.DateTime, nullable=True),
        sa.Column('updated_at', sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index('ix_pbs_pattern_regime', 'pattern_backtest_stats', ['pattern_id', 'regime'])


def downgrade() -> None:
    op.drop_index('ix_pbs_pattern_regime', 'pattern_backtest_stats')
    op.drop_table('pattern_backtest_stats')
    op.drop_index('ix_psr_score', 'pattern_scan_results')
    op.drop_index('ix_psr_sym_time', 'pattern_scan_results')
    op.drop_index('ix_psr_batch', 'pattern_scan_results')
    op.drop_table('pattern_scan_results')
