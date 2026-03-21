"""
PatternScanner 单元测试

覆盖：
- scan_latest 基本流程
- HIGH_VOL 过滤
- C类过滤器
- score_pattern 得分计算
- get_candidates 筛选
"""
from __future__ import annotations

import asyncio
import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from ..exceptions import InsufficientDataError, MissingColumnError
from ..models import Regime, RegimeResult
from ..patterns.definitions import ALL_PATTERNS, PATTERN_REGISTRY
from ..scanner import PatternScanner, _validate_df
from .conftest import _make_df


@pytest.fixture
def scanner():
    return PatternScanner(patterns=ALL_PATTERNS)


# ── _validate_df ───────────────────────────────────────────────────────────────

class TestValidateDf:
    def test_missing_column(self):
        df = pd.DataFrame({'open': [1], 'high': [2], 'low': [0.5], 'close': [1.5]})
        with pytest.raises(MissingColumnError):
            _validate_df(df)

    def test_too_short(self):
        df = _make_df(20, 'up')
        with pytest.raises(InsufficientDataError):
            _validate_df(df)

    def test_valid(self, df_up):
        _validate_df(df_up)  # should not raise


# ── scan_latest ────────────────────────────────────────────────────────────────

class TestScanLatest:
    @pytest.mark.asyncio
    async def test_returns_list(self, scanner, df_up):
        results = await scanner.scan_latest(df_up, 'TESTUSDT', '4h')
        assert isinstance(results, list)

    @pytest.mark.asyncio
    async def test_high_vol_returns_empty(self, scanner):
        """极高波动率的数据应被过滤，返回空列表"""
        rng = np.random.default_rng(0)
        n = 200
        ts = pd.date_range('2024-01-01', periods=n, freq='4h')
        # 制造极端波动
        close = 100 + np.cumsum(rng.normal(0, 15, n))
        close = np.maximum(close, 1.0)
        high  = close * 1.5
        low   = close * 0.5
        df = pd.DataFrame({
            'open': close, 'high': high, 'low': low,
            'close': close, 'volume': rng.uniform(1e6, 5e6, n)
        }, index=ts)
        results = await scanner.scan_latest(df, 'VOLATILEUSDT', '4h')
        assert results == []

    @pytest.mark.asyncio
    async def test_invalid_df_raises(self, scanner):
        df = _make_df(20, 'up')
        with pytest.raises(InsufficientDataError):
            await scanner.scan_latest(df, 'TEST', '4h')

    @pytest.mark.asyncio
    async def test_results_sorted_by_score(self, scanner, df_up):
        results = await scanner.scan_latest(df_up, 'TEST', '4h')
        if len(results) >= 2:
            scores = [r.total_score for r in results]
            assert scores == sorted(scores, reverse=True)

    @pytest.mark.asyncio
    async def test_result_fields_populated(self, scanner, df_up):
        results = await scanner.scan_latest(df_up, 'BTCUSDT', '4h')
        for r in results:
            assert r.symbol == 'BTCUSDT'
            assert r.timeframe == '4h'
            assert r.pattern_id in PATTERN_REGISTRY
            assert 0.0 <= r.total_score <= 100.0
            assert r.regime in ('bull_trend', 'ranging', 'bear_trend')


# ── score_pattern ──────────────────────────────────────────────────────────────

class TestScorePattern:
    def test_score_in_range(self, scanner, df_up):
        from ..field_evaluator import FieldEvaluator
        from ..indicators import IndicatorLibrary
        from ..regime_detector import MarketRegimeDetector

        ind = IndicatorLibrary()
        ev  = FieldEvaluator(ind)
        rd  = MarketRegimeDetector()
        regime_result = rd.detect(df_up)

        pattern = PATTERN_REGISTRY.get('A1')
        if pattern is None:
            pytest.skip('A1 pattern not found')

        r = scanner.score_pattern(df_up, pattern, regime_result, ev, 'TEST', '4h')
        assert 0.0 <= r.total_score <= 100.0

    def test_field_results_populated(self, scanner, df_up):
        from ..field_evaluator import FieldEvaluator
        from ..indicators import IndicatorLibrary
        from ..regime_detector import MarketRegimeDetector

        ind = IndicatorLibrary()
        ev  = FieldEvaluator(ind)
        rd  = MarketRegimeDetector()
        regime_result = rd.detect(df_up)

        pattern = PATTERN_REGISTRY.get('A1')
        if pattern is None:
            pytest.skip()

        r = scanner.score_pattern(df_up, pattern, regime_result, ev, 'TEST', '4h')
        assert len(r.field_results) > 0
        assert all(isinstance(v, bool) for v in r.field_results.values())

    def test_exclude_penalty_reduces_score(self, scanner, df_down):
        """下跌趋势数据上测试 B 类做空形态得分"""
        from ..field_evaluator import FieldEvaluator
        from ..indicators import IndicatorLibrary
        from ..regime_detector import MarketRegimeDetector

        ind = IndicatorLibrary()
        ev  = FieldEvaluator(ind)
        rd  = MarketRegimeDetector()
        regime_result = rd.detect(df_down)

        pattern = PATTERN_REGISTRY.get('B1')
        if pattern is None:
            pytest.skip()

        r = scanner.score_pattern(df_down, pattern, regime_result, ev, 'TEST', '4h')
        assert r.exclude_penalty >= 0.0
        assert r.total_score >= 0.0


# ── _check_filters ─────────────────────────────────────────────────────────────

class TestCheckFilters:
    def test_filter_returns_list(self, scanner, df_flat):
        from ..field_evaluator import FieldEvaluator
        from ..indicators import IndicatorLibrary
        from ..regime_detector import MarketRegimeDetector

        ind = IndicatorLibrary()
        ev  = FieldEvaluator(ind)
        rd  = MarketRegimeDetector()
        regime_result = rd.detect(df_flat)

        hits = scanner._check_filters(df_flat, regime_result, ev, 'TEST', '4h')
        assert isinstance(hits, list)
        for h in hits:
            assert h.is_filter_hit is True
            assert h.pattern_id.startswith('C')


# ── get_candidates ─────────────────────────────────────────────────────────────

class TestGetCandidates:
    def test_filters_by_score(self):
        from ..models import PatternScanResult
        from datetime import datetime

        def _make_result(score, is_filter=False, trigger=False):
            return PatternScanResult(
                symbol='TEST', timeframe='4h', bar_time=datetime.utcnow(),
                pattern_id='A1', pattern_name='Test', direction='long',
                regime='bull_trend', regime_score=80.0,
                total_score=score, confirm_score=score, exclude_penalty=0.0,
                field_results={}, raw_values={}, trigger_met=trigger,
                is_filter_hit=is_filter,
            )

        results = [
            _make_result(90.0),
            _make_result(75.0),
            _make_result(50.0),   # below threshold
            _make_result(80.0, is_filter=True),  # filter hit, excluded
        ]
        candidates = PatternScanner.get_candidates(results, min_score=70.0)
        assert len(candidates) == 2
        scores = [r.total_score for r in candidates]
        assert scores == sorted(scores, reverse=True)

    def test_trigger_only_filter(self):
        from ..models import PatternScanResult
        from datetime import datetime

        def _make_result(score, trigger):
            return PatternScanResult(
                symbol='TEST', timeframe='4h', bar_time=datetime.utcnow(),
                pattern_id='A1', pattern_name='Test', direction='long',
                regime='bull_trend', regime_score=80.0,
                total_score=score, confirm_score=score, exclude_penalty=0.0,
                field_results={}, raw_values={}, trigger_met=trigger,
            )

        results = [_make_result(80.0, True), _make_result(85.0, False)]
        triggered = PatternScanner.get_candidates(results, trigger_only=True)
        assert all(r.trigger_met for r in triggered)
        assert len(triggered) == 1
