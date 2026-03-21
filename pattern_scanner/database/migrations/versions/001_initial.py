"""Initial tables: kline_cache, symbol_universe, market_regime_log, data_fetch_log

Revision ID: 001
Revises:
Create Date: 2024-01-01
"""
from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        'kline_cache',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.String(20), nullable=False),
        sa.Column('interval', sa.String(10), nullable=False),
        sa.Column('open_time', sa.DateTime, nullable=False),
        sa.Column('open', sa.Float, nullable=False),
        sa.Column('high', sa.Float, nullable=False),
        sa.Column('low', sa.Float, nullable=False),
        sa.Column('close', sa.Float, nullable=False),
        sa.Column('volume', sa.Float, nullable=False),
        sa.Column('quote_volume', sa.Float, nullable=True),
        sa.Column('trade_count', sa.Integer, nullable=True),
        sa.Column('taker_buy_vol', sa.Float, nullable=True),
        sa.UniqueConstraint('symbol', 'interval', 'open_time', name='uq_kline'),
    )
    op.create_index('ix_kline_sym_iv', 'kline_cache', ['symbol', 'interval'])
    op.create_index('ix_kline_sym_iv_time', 'kline_cache', ['symbol', 'interval', 'open_time'])

    op.create_table(
        'symbol_universe',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.String(20), unique=True, nullable=False),
        sa.Column('is_active', sa.Boolean, default=True),
        sa.Column('is_scannable', sa.Boolean, default=True),
        sa.Column('exclude_reason', sa.String(50), nullable=True),
        sa.Column('contract_type', sa.String(20), nullable=True),
        sa.Column('quote_asset', sa.String(10), nullable=True),
        sa.Column('margin_asset', sa.String(10), nullable=True),
        sa.Column('source_exchange', sa.String(20), default='binance_usdm'),
        sa.Column('listed_at', sa.DateTime, nullable=True),
        sa.Column('delisted_at', sa.DateTime, nullable=True),
        sa.Column('first_seen_at', sa.DateTime, server_default=sa.func.now()),
        sa.Column('last_seen_at', sa.DateTime, server_default=sa.func.now()),
    )
    op.create_index('ix_su_active', 'symbol_universe', ['is_active'])

    op.create_table(
        'market_regime_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('symbol', sa.String(20), nullable=False),
        sa.Column('timeframe', sa.String(10), nullable=False),
        sa.Column('bar_time', sa.DateTime, nullable=False),
        sa.Column('regime', sa.String(20), nullable=True),
        sa.Column('regime_score', sa.Float, nullable=True),
        sa.Column('trend_score', sa.Float, nullable=True),
        sa.Column('vol_score', sa.Float, nullable=True),
        sa.Column('volume_score', sa.Float, nullable=True),
        sa.Column('btc_score', sa.Float, nullable=True),
        sa.Column('atr_ratio', sa.Float, nullable=True),
        sa.Column('bb_width', sa.Float, nullable=True),
        sa.Column('ma_bull_align', sa.Boolean, nullable=True),
        sa.Column('ma_bear_align', sa.Boolean, nullable=True),
        sa.UniqueConstraint('symbol', 'timeframe', 'bar_time', name='uq_regime'),
    )
    op.create_index('ix_mrl_sym_time', 'market_regime_log', ['symbol', 'bar_time'])

    op.create_table(
        'data_fetch_log',
        sa.Column('id', sa.Integer, primary_key=True),
        sa.Column('batch_id', sa.String(36), nullable=True),
        sa.Column('interval', sa.String(10), nullable=True),
        sa.Column('triggered_at', sa.DateTime, server_default=sa.func.now()),
        sa.Column('symbols_total', sa.Integer, nullable=True),
        sa.Column('symbols_success', sa.Integer, nullable=True),
        sa.Column('symbols_failed', sa.Integer, nullable=True),
        sa.Column('failed_symbols', sa.JSON, nullable=True),
        sa.Column('duration_sec', sa.Float, nullable=True),
        sa.Column('status', sa.String(20), nullable=True),
    )


def downgrade() -> None:
    op.drop_table('data_fetch_log')
    op.drop_index('ix_mrl_sym_time', 'market_regime_log')
    op.drop_table('market_regime_log')
    op.drop_index('ix_su_active', 'symbol_universe')
    op.drop_table('symbol_universe')
    op.drop_index('ix_kline_sym_iv_time', 'kline_cache')
    op.drop_index('ix_kline_sym_iv', 'kline_cache')
    op.drop_table('kline_cache')
