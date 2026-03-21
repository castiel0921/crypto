from __future__ import annotations

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Index, Integer,
    String, Text, UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSON as PG_JSON
from sqlalchemy.types import JSON
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.sql import func


class Base(DeclarativeBase):
    pass


# ── 1. K 线缓存 ──────────────────────────────────────────────────────────────

class KlineCacheORM(Base):
    __tablename__ = 'kline_cache'
    id        = Column(Integer, primary_key=True)
    symbol    = Column(String(20), nullable=False)
    interval  = Column(String(10), nullable=False)
    open_time = Column(DateTime, nullable=False)
    open      = Column(Float, nullable=False)
    high      = Column(Float, nullable=False)
    low       = Column(Float, nullable=False)
    close     = Column(Float, nullable=False)
    volume    = Column(Float, nullable=False)
    # 预留扩展字段 (P2-15)
    quote_volume  = Column(Float, nullable=True)
    trade_count   = Column(Integer, nullable=True)
    taker_buy_vol = Column(Float, nullable=True)
    __table_args__ = (
        UniqueConstraint('symbol', 'interval', 'open_time', name='uq_kline'),
        Index('ix_kline_sym_iv', 'symbol', 'interval'),
        Index('ix_kline_sym_iv_time', 'symbol', 'interval', 'open_time'),
    )


# ── 2. 永久原始 K 线（用于回测）────────────────────────────────────────────

class RawKlineStoreORM(Base):
    __tablename__ = 'raw_kline_store'
    id        = Column(Integer, primary_key=True)
    symbol    = Column(String(20), nullable=False)
    interval  = Column(String(10), nullable=False)
    open_time = Column(DateTime, nullable=False)
    open      = Column(Float, nullable=False)
    high      = Column(Float, nullable=False)
    low       = Column(Float, nullable=False)
    close     = Column(Float, nullable=False)
    volume    = Column(Float, nullable=False)
    __table_args__ = (
        UniqueConstraint('symbol', 'interval', 'open_time', name='uq_raw_kline'),
        Index('ix_raw_sym_iv_time', 'symbol', 'interval', 'open_time'),
    )


# ── 3. 标的管理 ───────────────────────────────────────────────────────────────

class SymbolUniverseORM(Base):
    __tablename__ = 'symbol_universe'
    id              = Column(Integer, primary_key=True)
    symbol          = Column(String(20), unique=True, nullable=False)
    is_active       = Column(Boolean, default=True)
    is_scannable    = Column(Boolean, default=True)
    exclude_reason  = Column(String(50), nullable=True)
    contract_type   = Column(String(20), nullable=True)
    quote_asset     = Column(String(10), nullable=True)
    margin_asset    = Column(String(10), nullable=True)
    source_exchange = Column(String(20), default='binance_usdm')
    listed_at       = Column(DateTime, nullable=True)
    delisted_at     = Column(DateTime, nullable=True)
    first_seen_at   = Column(DateTime, server_default=func.now())
    last_seen_at    = Column(DateTime, server_default=func.now(), onupdate=func.now())
    __table_args__ = (Index('ix_su_active', 'is_active'),)


# ── 4. 数据拉取日志 ───────────────────────────────────────────────────────────

class DataFetchLogORM(Base):
    __tablename__ = 'data_fetch_log'
    id              = Column(Integer, primary_key=True)
    batch_id        = Column(String(36), nullable=True)
    interval        = Column(String(10), nullable=True)
    triggered_at    = Column(DateTime, server_default=func.now())
    symbols_total   = Column(Integer, nullable=True)
    symbols_success = Column(Integer, nullable=True)
    symbols_failed  = Column(Integer, nullable=True)
    failed_symbols  = Column(JSON, nullable=True)
    duration_sec    = Column(Float, nullable=True)
    status          = Column(String(20), nullable=True)


# ── 5. 形态扫描结果 ───────────────────────────────────────────────────────────

class PatternScanResultORM(Base):
    __tablename__ = 'pattern_scan_results'
    id                  = Column(Integer, primary_key=True)
    symbol              = Column(String(20), nullable=False)
    timeframe           = Column(String(10), nullable=False)
    bar_time            = Column(DateTime, nullable=False)
    pattern_id          = Column(String(10), nullable=True)
    pattern_name        = Column(String(60), nullable=True)
    direction           = Column(String(10), nullable=True)
    regime              = Column(String(20), nullable=True)
    regime_score        = Column(Float, nullable=True)
    total_score         = Column(Float, nullable=True)
    confirm_score       = Column(Float, nullable=True)
    exclude_penalty     = Column(Float, nullable=True)
    trigger_met         = Column(Boolean, nullable=True)
    trigger_type        = Column(String(30), nullable=True)
    invalidated         = Column(Boolean, default=False)
    invalidation_reason = Column(String(50), nullable=True)
    is_filter_hit       = Column(Boolean, default=False)
    field_results       = Column(JSON, nullable=True)
    raw_values          = Column(JSON, nullable=True)
    scan_batch_id       = Column(String(36), nullable=True)
    rule_version        = Column(String(20), nullable=True)
    llm_confidence      = Column(String(10), nullable=True)
    llm_risk            = Column(Text, nullable=True)
    llm_enter_pool      = Column(Boolean, nullable=True)
    llm_reasoning       = Column(Text, nullable=True)
    llm_prompt_ver      = Column(String(10), nullable=True)
    llm_reviewed_at     = Column(DateTime, nullable=True)
    created_at          = Column(DateTime, server_default=func.now())
    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'bar_time', 'pattern_id', name='uq_scan'),
        Index('ix_psr_batch',    'scan_batch_id'),
        Index('ix_psr_sym_time', 'symbol', 'bar_time'),
        Index('ix_psr_score',    'total_score'),
        Index('ix_psr_llm_conf', 'llm_confidence'),
    )


# ── 6. 市场体制日志 ───────────────────────────────────────────────────────────

class MarketRegimeLogORM(Base):
    __tablename__ = 'market_regime_log'
    id            = Column(Integer, primary_key=True)
    symbol        = Column(String(20), nullable=False)
    timeframe     = Column(String(10), nullable=False)
    bar_time      = Column(DateTime, nullable=False)
    regime        = Column(String(20), nullable=True)
    regime_score  = Column(Float, nullable=True)
    trend_score   = Column(Float, nullable=True)
    vol_score     = Column(Float, nullable=True)
    volume_score  = Column(Float, nullable=True)
    btc_score     = Column(Float, nullable=True)
    atr_ratio     = Column(Float, nullable=True)
    bb_width      = Column(Float, nullable=True)
    ma_bull_align = Column(Boolean, nullable=True)
    ma_bear_align = Column(Boolean, nullable=True)
    __table_args__ = (
        UniqueConstraint('symbol', 'timeframe', 'bar_time', name='uq_regime'),
        Index('ix_mrl_sym_time', 'symbol', 'bar_time'),
    )


# ── 7. 形态回测统计 ───────────────────────────────────────────────────────────

class PatternBacktestStatsORM(Base):
    __tablename__ = 'pattern_backtest_stats'
    id                     = Column(Integer, primary_key=True)
    pattern_id             = Column(String(10), nullable=False)
    regime                 = Column(String(20), nullable=True)
    timeframe              = Column(String(10), nullable=True)
    forward_bars           = Column(Integer, default=10)
    trigger_only           = Column(Boolean, default=True)
    sample_size            = Column(Integer, nullable=True)
    win_rate               = Column(Float, nullable=True)
    avg_return             = Column(Float, nullable=True)
    avg_holding_bars       = Column(Float, nullable=True)
    max_drawdown           = Column(Float, nullable=True)
    sharpe_like            = Column(Float, nullable=True)
    llm_high_conf_win_rate = Column(Float, nullable=True)
    stat_period_start      = Column(DateTime, nullable=True)
    stat_period_end        = Column(DateTime, nullable=True)
    updated_at             = Column(DateTime, server_default=func.now(), onupdate=func.now())
    __table_args__ = (Index('ix_pbs_pattern_regime', 'pattern_id', 'regime'),)


# ── 8. LLM Analyst 报告 ───────────────────────────────────────────────────────

class LLMAnalystReportORM(Base):
    __tablename__ = 'llm_analyst_reports'
    id              = Column(Integer, primary_key=True)
    scan_batch_id   = Column(String(36), nullable=False)
    report_time     = Column(DateTime, server_default=func.now())
    btc_regime      = Column(String(20), nullable=True)
    btc_narrative   = Column(Text, nullable=True)
    top_long        = Column(JSON, nullable=True)
    top_short       = Column(JSON, nullable=True)
    warnings        = Column(JSON, nullable=True)
    market_summary  = Column(Text, nullable=True)
    candidate_count = Column(Integer, nullable=True)
    prompt_version  = Column(String(10), nullable=True)
    __table_args__ = (Index('ix_lar_batch', 'scan_batch_id'),)


# ── 9. LLM Prompt 模板 ────────────────────────────────────────────────────────

class LLMPromptTemplateORM(Base):
    __tablename__ = 'llm_prompt_templates'
    id         = Column(Integer, primary_key=True)
    module     = Column(String(20), nullable=True)
    version    = Column(String(10), nullable=True)
    content    = Column(Text, nullable=False)
    is_active  = Column(Boolean, default=True)
    created_at = Column(DateTime, server_default=func.now())
    notes      = Column(Text, nullable=True)
    __table_args__ = (UniqueConstraint('module', 'version', name='uq_prompt_mv'),)


# ── 10. Pipeline 运行日志 ─────────────────────────────────────────────────────

class PipelineRunLogORM(Base):
    __tablename__ = 'pipeline_run_log'
    id                 = Column(Integer, primary_key=True)
    job_id             = Column(String(36), unique=True, nullable=False)
    batch_id           = Column(String(36), nullable=True)
    interval           = Column(String(10), nullable=True)
    triggered_at       = Column(DateTime, nullable=False)
    finished_at        = Column(DateTime, nullable=True)
    stage              = Column(String(30), nullable=True)
    status             = Column(String(20), nullable=True)
    symbols_total      = Column(Integer, nullable=True)
    symbols_fetched    = Column(Integer, nullable=True)
    symbols_scannable  = Column(Integer, nullable=True)
    symbols_skipped    = Column(Integer, nullable=True)
    patterns_found     = Column(Integer, nullable=True)
    llm_reviewed       = Column(Integer, nullable=True)
    llm_success        = Column(Integer, nullable=True)
    llm_timeout        = Column(Integer, nullable=True)
    error_stage        = Column(String(30), nullable=True)
    error_message      = Column(Text, nullable=True)
    failed_symbols     = Column(JSON, nullable=True)
    duration_sec       = Column(Float, nullable=True)
    __table_args__ = (Index('ix_prl_triggered', 'triggered_at'),)
