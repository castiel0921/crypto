from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import pandas as pd
from sqlalchemy import delete, select, text, update

from ..models import (
    DataFetchLog,
    PatternBacktestStats,
    PatternScanResult,
    PipelineRunLog,
    SymbolCacheHealth,
)
from .models import (
    DataFetchLogORM,
    KlineCacheORM,
    LLMAnalystReportORM,
    MarketRegimeLogORM,
    PatternBacktestStatsORM,
    PatternScanResultORM,
    PipelineRunLogORM,
    RawKlineStoreORM,
    SymbolUniverseORM,
)
from .session import get_session

logger = logging.getLogger(__name__)

MIN_BARS = 150
KLINE_KEEP = 500


class PatternRepository:
    """无状态 repository，不持有 session。所有方法内部通过 get_session() 创建 session scope。"""

    # ── K 线数据 ────────────────────────────────────────────────────────────

    async def upsert_klines(self, symbol: str, interval: str, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        async with get_session() as session:
            rows = [
                {
                    'symbol':    symbol,
                    'interval':  interval,
                    'open_time': idx.to_pydatetime().replace(tzinfo=None),
                    'open':      float(row.open),
                    'high':      float(row.high),
                    'low':       float(row.low),
                    'close':     float(row.close),
                    'volume':    float(row.volume),
                }
                for idx, row in df.iterrows()
            ]
            # upsert via INSERT OR IGNORE / ON CONFLICT DO NOTHING
            for r in rows:
                existing = await session.execute(
                    select(KlineCacheORM).where(
                        KlineCacheORM.symbol    == r['symbol'],
                        KlineCacheORM.interval  == r['interval'],
                        KlineCacheORM.open_time == r['open_time'],
                    )
                )
                if existing.scalar_one_or_none() is None:
                    session.add(KlineCacheORM(**r))
            return len(rows)

    async def upsert_klines_raw(self, symbol: str, interval: str, df: pd.DataFrame) -> int:
        """同时写入 raw_kline_store（永久保存）"""
        if df.empty:
            return 0
        async with get_session() as session:
            rows = [
                {
                    'symbol':    symbol,
                    'interval':  interval,
                    'open_time': idx.to_pydatetime().replace(tzinfo=None),
                    'open':      float(row.open),
                    'high':      float(row.high),
                    'low':       float(row.low),
                    'close':     float(row.close),
                    'volume':    float(row.volume),
                }
                for idx, row in df.iterrows()
            ]
            for r in rows:
                existing = await session.execute(
                    select(RawKlineStoreORM).where(
                        RawKlineStoreORM.symbol    == r['symbol'],
                        RawKlineStoreORM.interval  == r['interval'],
                        RawKlineStoreORM.open_time == r['open_time'],
                    )
                )
                if existing.scalar_one_or_none() is None:
                    session.add(RawKlineStoreORM(**r))
            return len(rows)

    async def get_klines(
        self, symbol: str, interval: str, limit: int = 300
    ) -> pd.DataFrame:
        async with get_session() as session:
            result = await session.execute(
                select(KlineCacheORM)
                .where(
                    KlineCacheORM.symbol   == symbol,
                    KlineCacheORM.interval == interval,
                )
                .order_by(KlineCacheORM.open_time.desc())
                .limit(limit)
            )
            rows = result.scalars().all()
        if not rows:
            return pd.DataFrame()
        return self._rows_to_df(rows)

    async def get_klines_batch(
        self,
        symbols: list[str],
        interval: str,
        limit: int = 300,
    ) -> dict[str, pd.DataFrame]:
        """单次查询拉取多个标的最新 K 线窗口（窗口函数）"""
        if not symbols:
            return {}
        async with get_session() as session:
            # 使用 ROW_NUMBER 窗口函数
            q = text("""
                SELECT * FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time DESC) AS rn
                    FROM kline_cache
                    WHERE symbol IN :syms AND interval = :iv
                ) t
                WHERE rn <= :lim
                ORDER BY symbol, open_time ASC
            """)
            result = await session.execute(
                q, {'syms': tuple(symbols), 'iv': interval, 'lim': limit}
            )
            rows = result.fetchall()

        if not rows:
            return {}

        # 按 symbol 分组
        data: dict[str, list] = {}
        for row in rows:
            sym = row.symbol
            data.setdefault(sym, []).append(row)

        out: dict[str, pd.DataFrame] = {}
        for sym, sym_rows in data.items():
            out[sym] = self._rows_to_df(sym_rows, ascending=True)
        return out

    async def cleanup_old_klines(self, symbol: str, interval: str, keep: int = KLINE_KEEP) -> int:
        async with get_session() as session:
            subq = (
                select(KlineCacheORM.open_time)
                .where(
                    KlineCacheORM.symbol   == symbol,
                    KlineCacheORM.interval == interval,
                )
                .order_by(KlineCacheORM.open_time.desc())
                .offset(keep)
                .limit(1)
            )
            cutoff_result = await session.execute(subq)
            cutoff = cutoff_result.scalar_one_or_none()
            if cutoff is None:
                return 0
            result = await session.execute(
                delete(KlineCacheORM).where(
                    KlineCacheORM.symbol    == symbol,
                    KlineCacheORM.interval  == interval,
                    KlineCacheORM.open_time <= cutoff,
                )
            )
            return result.rowcount

    async def count_kline_cache(self) -> int:
        async with get_session() as session:
            result = await session.execute(text('SELECT COUNT(*) FROM kline_cache'))
            return result.scalar() or 0

    # ── 标的管理 ────────────────────────────────────────────────────────────

    async def upsert_symbols(self, symbol_infos: list[dict]) -> None:
        """upsert symbol_universe；symbol_infos 为含 symbol/is_active/exclude_reason 等的 dict 列表"""
        async with get_session() as session:
            for info in symbol_infos:
                sym = info['symbol']
                existing = await session.execute(
                    select(SymbolUniverseORM).where(SymbolUniverseORM.symbol == sym)
                )
                row = existing.scalar_one_or_none()
                now = datetime.now(timezone.utc).replace(tzinfo=None)
                if row is None:
                    session.add(SymbolUniverseORM(
                        symbol          = sym,
                        is_active       = info.get('is_active', True),
                        is_scannable    = info.get('is_scannable', True),
                        exclude_reason  = info.get('exclude_reason'),
                        contract_type   = info.get('contract_type'),
                        quote_asset     = info.get('quote_asset'),
                        margin_asset    = info.get('margin_asset'),
                        source_exchange = info.get('source_exchange', 'binance_usdm'),
                        listed_at       = info.get('listed_at'),
                        first_seen_at   = now,
                        last_seen_at    = now,
                    ))
                else:
                    row.is_active      = info.get('is_active', True)
                    row.is_scannable   = info.get('is_scannable', True)
                    row.exclude_reason = info.get('exclude_reason')
                    row.last_seen_at   = now
                    if info.get('is_active') is False and row.delisted_at is None:
                        row.delisted_at = now

    async def get_active_symbols(self) -> list[str]:
        async with get_session() as session:
            result = await session.execute(
                select(SymbolUniverseORM.symbol).where(SymbolUniverseORM.is_active == True)
            )
            return [r[0] for r in result.all()]

    async def get_symbol_health(
        self,
        symbol: str,
        interval: str,
        expected_period_sec: int = 14400,
    ) -> SymbolCacheHealth:
        async with get_session() as session:
            result = await session.execute(
                select(KlineCacheORM)
                .where(
                    KlineCacheORM.symbol   == symbol,
                    KlineCacheORM.interval == interval,
                )
                .order_by(KlineCacheORM.open_time.asc())
            )
            rows = result.scalars().all()

        bars_count = len(rows)
        if bars_count == 0:
            return SymbolCacheHealth(
                symbol=symbol, bars_count=0,
                latest_time=None, oldest_time=None,
                gap_count=0, is_fresh=False, is_scannable=False,
            )

        times = [r.open_time for r in rows]
        oldest = times[0]
        latest = times[-1]

        # 计算时间缺口
        gap_count = 0
        threshold = expected_period_sec * 1.5
        for i in range(1, len(times)):
            diff = (times[i] - times[i-1]).total_seconds()
            if diff > threshold:
                gap_count += 1

        now = datetime.utcnow()
        is_fresh = (now - latest).total_seconds() < expected_period_sec * 2
        is_scannable = bars_count >= MIN_BARS

        return SymbolCacheHealth(
            symbol=symbol, bars_count=bars_count,
            latest_time=latest, oldest_time=oldest,
            gap_count=gap_count, is_fresh=is_fresh, is_scannable=is_scannable,
        )

    async def get_unhealthy_symbols(self, interval: str, min_bars: int = MIN_BARS) -> list[str]:
        async with get_session() as session:
            result = await session.execute(
                select(SymbolUniverseORM.symbol).where(SymbolUniverseORM.is_active == True)
            )
            all_symbols = [r[0] for r in result.all()]

        unhealthy = []
        for sym in all_symbols:
            health = await self.get_symbol_health(sym, interval)
            if not health.is_scannable or not health.is_fresh:
                unhealthy.append(sym)
        return unhealthy

    # ── 扫描结果 ────────────────────────────────────────────────────────────

    async def bulk_save(self, results: list[PatternScanResult]) -> list[int]:
        if not results:
            return []
        db_ids = []
        async with get_session() as session:
            for r in results:
                # 先查是否已存在（upsert）
                bar_time = r.bar_time.replace(tzinfo=None) if (hasattr(r.bar_time, 'tzinfo') and r.bar_time.tzinfo) else r.bar_time
                existing = await session.execute(
                    select(PatternScanResultORM).where(
                        PatternScanResultORM.symbol     == r.symbol,
                        PatternScanResultORM.timeframe  == r.timeframe,
                        PatternScanResultORM.bar_time   == bar_time,
                        PatternScanResultORM.pattern_id == r.pattern_id,
                    )
                )
                row = existing.scalar_one_or_none()
                if row is None:
                    orm = PatternScanResultORM(
                        symbol              = r.symbol,
                        timeframe           = r.timeframe,
                        bar_time            = bar_time,
                        pattern_id          = r.pattern_id,
                        pattern_name        = r.pattern_name,
                        direction           = r.direction,
                        regime              = r.regime,
                        regime_score        = r.regime_score,
                        total_score         = r.total_score,
                        confirm_score       = r.confirm_score,
                        exclude_penalty     = r.exclude_penalty,
                        trigger_met         = r.trigger_met,
                        trigger_type        = r.trigger_type,
                        invalidated         = r.invalidated,
                        invalidation_reason = r.invalidation_reason,
                        is_filter_hit       = r.is_filter_hit,
                        field_results       = r.field_results,
                        raw_values          = r.raw_values,
                        scan_batch_id       = r.scan_batch_id,
                        rule_version        = r.rule_version,
                    )
                    session.add(orm)
                    await session.flush()
                    db_ids.append(orm.id)
                else:
                    db_ids.append(row.id)
        return db_ids

    async def get_by_id(self, result_id: int) -> Optional[PatternScanResult]:
        async with get_session() as session:
            result = await session.execute(
                select(PatternScanResultORM).where(PatternScanResultORM.id == result_id)
            )
            row = result.scalar_one_or_none()
        if row is None:
            return None
        return self._orm_to_result(row)

    async def get_by_symbol_time_range(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[PatternScanResult]:
        async with get_session() as session:
            result = await session.execute(
                select(PatternScanResultORM).where(
                    PatternScanResultORM.symbol   == symbol,
                    PatternScanResultORM.bar_time >= start,
                    PatternScanResultORM.bar_time <= end,
                ).order_by(PatternScanResultORM.bar_time.asc())
            )
            rows = result.scalars().all()
        return [self._orm_to_result(r) for r in rows]

    async def get_candidates_for_analyst(
        self,
        scan_batch_id: str,
        min_score: float = 70.0,
        limit: int = 30,
        direction: Optional[str] = None,
    ) -> list[dict]:
        async with get_session() as session:
            q = select(PatternScanResultORM).where(
                PatternScanResultORM.scan_batch_id == scan_batch_id,
                PatternScanResultORM.total_score   >= min_score,
                PatternScanResultORM.is_filter_hit == False,
            )
            if direction:
                q = q.where(PatternScanResultORM.direction == direction)
            q = q.order_by(PatternScanResultORM.total_score.desc()).limit(limit)
            result = await session.execute(q)
            rows = result.scalars().all()
        return [
            {
                'symbol':         r.symbol,
                'pattern_id':     r.pattern_id,
                'pattern_name':   r.pattern_name,
                'direction':      r.direction,
                'regime':         r.regime,
                'total_score':    r.total_score,
                'trigger_met':    r.trigger_met,
                'llm_confidence': r.llm_confidence,
                'bar_time':       r.bar_time.isoformat() if r.bar_time else None,
            }
            for r in rows
        ]

    # ── LLM 回写 ────────────────────────────────────────────────────────────

    async def update_llm_review(
        self,
        result_id: int,
        confidence: str,
        risk: str,
        enter_pool: bool,
        reasoning: str,
        prompt_version: str,
    ) -> None:
        async with get_session() as session:
            await session.execute(
                update(PatternScanResultORM)
                .where(PatternScanResultORM.id == result_id)
                .values(
                    llm_confidence  = confidence,
                    llm_risk        = risk,
                    llm_enter_pool  = enter_pool,
                    llm_reasoning   = reasoning,
                    llm_prompt_ver  = prompt_version,
                    llm_reviewed_at = datetime.utcnow(),
                )
            )

    async def save_analyst_report(
        self,
        scan_batch_id: str,
        output,
        btc_regime: str,
        btc_narrative: str,
        candidate_count: int,
    ) -> None:
        async with get_session() as session:
            session.add(LLMAnalystReportORM(
                scan_batch_id   = scan_batch_id,
                btc_regime      = btc_regime,
                btc_narrative   = btc_narrative,
                top_long        = [vars(c) for c in output.top_long],
                top_short       = [vars(c) for c in output.top_short],
                warnings        = output.warnings,
                market_summary  = output.market_summary,
                candidate_count = candidate_count,
                prompt_version  = output.prompt_version,
            ))

    async def get_stats_by_regime(self, pattern_id: str, timeframe: str) -> list[dict]:
        async with get_session() as session:
            result = await session.execute(
                select(PatternBacktestStatsORM).where(
                    PatternBacktestStatsORM.pattern_id == pattern_id,
                    PatternBacktestStatsORM.timeframe  == timeframe,
                )
            )
            rows = result.scalars().all()
        return [
            {
                'regime':    r.regime,
                'win_rate':  r.win_rate,
                'avg_return': r.avg_return,
                'sample_size': r.sample_size,
            }
            for r in rows
        ]

    async def upsert_backtest_stats(self, stats: list[PatternBacktestStats]) -> None:
        async with get_session() as session:
            for s in stats:
                existing = await session.execute(
                    select(PatternBacktestStatsORM).where(
                        PatternBacktestStatsORM.pattern_id   == s.pattern_id,
                        PatternBacktestStatsORM.regime       == s.regime,
                        PatternBacktestStatsORM.timeframe    == s.timeframe,
                        PatternBacktestStatsORM.forward_bars == s.forward_bars,
                    )
                )
                row = existing.scalar_one_or_none()
                if row is None:
                    session.add(PatternBacktestStatsORM(
                        pattern_id             = s.pattern_id,
                        regime                 = s.regime,
                        timeframe              = s.timeframe,
                        forward_bars           = s.forward_bars,
                        trigger_only           = s.trigger_only,
                        sample_size            = s.sample_size,
                        win_rate               = s.win_rate,
                        avg_return             = s.avg_return,
                        avg_holding_bars       = s.avg_holding_bars,
                        max_drawdown           = s.max_drawdown,
                        sharpe_like            = s.sharpe_like,
                        llm_high_conf_win_rate = s.llm_high_conf_win_rate,
                        stat_period_start      = s.stat_period_start,
                        stat_period_end        = s.stat_period_end,
                    ))
                else:
                    row.sample_size            = s.sample_size
                    row.win_rate               = s.win_rate
                    row.avg_return             = s.avg_return
                    row.max_drawdown           = s.max_drawdown
                    row.sharpe_like            = s.sharpe_like
                    row.stat_period_start      = s.stat_period_start
                    row.stat_period_end        = s.stat_period_end

    # ── 拉取日志 ────────────────────────────────────────────────────────────

    async def save_fetch_log(self, log: DataFetchLog) -> None:
        async with get_session() as session:
            session.add(DataFetchLogORM(
                batch_id        = log.batch_id,
                interval        = log.interval,
                symbols_total   = log.symbols_total,
                symbols_success = log.symbols_success,
                symbols_failed  = log.symbols_failed,
                failed_symbols  = log.failed_symbols,
                duration_sec    = log.duration_sec,
                status          = log.status,
            ))

    async def start_job_log(self, job_id: str, batch_id: str, interval: str) -> None:
        async with get_session() as session:
            session.add(PipelineRunLogORM(
                job_id       = job_id,
                batch_id     = batch_id,
                interval     = interval,
                triggered_at = datetime.utcnow(),
                status       = 'running',
                stage        = 'init',
            ))

    async def update_job_stage(self, job_id: str, stage: str) -> None:
        async with get_session() as session:
            await session.execute(
                update(PipelineRunLogORM)
                .where(PipelineRunLogORM.job_id == job_id)
                .values(stage=stage)
            )

    async def finish_job_log(
        self,
        job_id: str,
        status: str,
        stats: Optional[dict] = None,
        **kwargs,
    ) -> None:
        values = {
            'status':      status,
            'finished_at': datetime.utcnow(),
            **kwargs,
        }
        if stats:
            values.update({
                'symbols_total':     stats.get('symbols_total'),
                'symbols_fetched':   stats.get('symbols_fetched'),
                'symbols_scannable': stats.get('symbols_scannable'),
                'patterns_found':    stats.get('patterns_found'),
                'llm_reviewed':      stats.get('llm_reviewed_count'),
                'llm_success':       stats.get('llm_success_count'),
                'llm_timeout':       stats.get('llm_timeout_count'),
                'duration_sec':      stats.get('duration_total_sec'),
            })
        async with get_session() as session:
            await session.execute(
                update(PipelineRunLogORM)
                .where(PipelineRunLogORM.job_id == job_id)
                .values(**values)
            )

    async def get_stale_jobs(self, stale_minutes: int = 30) -> list[PipelineRunLog]:
        cutoff = datetime.utcnow() - timedelta(minutes=stale_minutes)
        async with get_session() as session:
            result = await session.execute(
                select(PipelineRunLogORM).where(
                    PipelineRunLogORM.status       == 'running',
                    PipelineRunLogORM.triggered_at <  cutoff,
                )
            )
            rows = result.scalars().all()
        return [
            PipelineRunLog(
                job_id       = r.job_id,
                batch_id     = r.batch_id or '',
                interval     = r.interval or '',
                triggered_at = r.triggered_at,
                status       = r.status,
                stage        = r.stage,
            )
            for r in rows
        ]

    async def get_scan_history(
        self,
        pattern_ids:  Optional[list[str]] = None,
        timeframes:   Optional[list[str]] = None,
        trigger_only: bool = False,
        limit:        int = 10000,
    ) -> list[dict]:
        """获取历史扫描记录，用于回测统计计算"""
        async with get_session() as session:
            q = select(PatternScanResultORM).where(
                PatternScanResultORM.is_filter_hit == False,
            )
            if pattern_ids:
                q = q.where(PatternScanResultORM.pattern_id.in_(pattern_ids))
            if timeframes:
                q = q.where(PatternScanResultORM.timeframe.in_(timeframes))
            if trigger_only:
                q = q.where(PatternScanResultORM.trigger_met == True)
            q = q.order_by(PatternScanResultORM.bar_time.asc()).limit(limit)
            result = await session.execute(q)
            rows = result.scalars().all()
        return [
            {
                'symbol':      r.symbol,
                'pattern_id':  r.pattern_id,
                'timeframe':   r.timeframe,
                'regime':      r.regime,
                'direction':   r.direction,
                'total_score': r.total_score,
                'trigger_met': r.trigger_met,
                'bar_time':    r.bar_time,
                'llm_confidence': r.llm_confidence,
            }
            for r in rows
        ]

    # ── 内部辅助 ────────────────────────────────────────────────────────────

    @staticmethod
    def _rows_to_df(rows, ascending: bool = False) -> pd.DataFrame:
        data = {
            'open':   [r.open   for r in rows],
            'high':   [r.high   for r in rows],
            'low':    [r.low    for r in rows],
            'close':  [r.close  for r in rows],
            'volume': [r.volume for r in rows],
        }
        idx = pd.DatetimeIndex(
            [r.open_time for r in rows], name='open_time'
        ).tz_localize('UTC')
        df = pd.DataFrame(data, index=idx)
        return df.sort_index(ascending=ascending)

    @staticmethod
    def _orm_to_result(row: PatternScanResultORM) -> PatternScanResult:
        bar_time = row.bar_time
        if bar_time and not hasattr(bar_time, 'tzinfo'):
            bar_time = bar_time
        return PatternScanResult(
            symbol              = row.symbol,
            timeframe           = row.timeframe,
            bar_time            = bar_time,
            pattern_id          = row.pattern_id or '',
            pattern_name        = row.pattern_name or '',
            direction           = row.direction or '',
            regime              = row.regime or '',
            regime_score        = row.regime_score or 0.0,
            total_score         = row.total_score or 0.0,
            confirm_score       = row.confirm_score or 0.0,
            exclude_penalty     = row.exclude_penalty or 0.0,
            field_results       = row.field_results or {},
            raw_values          = row.raw_values or {},
            trigger_met         = row.trigger_met or False,
            trigger_type        = row.trigger_type,
            invalidated         = row.invalidated or False,
            invalidation_reason = row.invalidation_reason,
            is_filter_hit       = row.is_filter_hit or False,
            scan_batch_id       = row.scan_batch_id,
            db_id               = row.id,
            llm_confidence      = row.llm_confidence,
            llm_enter_pool      = row.llm_enter_pool,
            llm_risk            = row.llm_risk,
            llm_reasoning       = row.llm_reasoning,
            rule_version        = row.rule_version,
        )
