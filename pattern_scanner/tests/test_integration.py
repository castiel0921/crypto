"""
集成测试 — 8个端到端场景

场景：
1. 上升趋势 + 全市场形态扫描（验证不崩溃）
2. 下跌趋势 + 过滤器命中
3. 高波动率 → 直接返回空
4. 数据不足 → 异常
5. 全形态定义有效性（字段合法）
6. 体制检测一致性（bull_trend → 不应输出 bear 形态）
7. 并发安全（多个 symbol 并发扫描）
8. 形态得分归一化（0-100范围内）
"""
from __future__ import annotations

import asyncio
import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from ..exceptions import InsufficientDataError
from ..indicators import IndicatorLibrary
from ..models import Regime
from ..patterns.definitions import ALL_PATTERNS, PATTERN_REGISTRY
from ..regime_detector import MarketRegimeDetector
from ..scanner import PatternScanner, _validate_df
from .conftest import _make_df


# ── Scenario 1: 上升趋势全扫描 ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario1_uptrend_scan_no_crash():
    scanner = PatternScanner()
    df = _make_df(300, 'up')
    results = await scanner.scan_latest(df, 'BTCUSDT', '4h')
    assert isinstance(results, list)
    # 验证所有返回结果均有效
    for r in results:
        assert 0.0 <= r.total_score <= 100.0
        assert r.symbol == 'BTCUSDT'
        assert r.pattern_id in PATTERN_REGISTRY


# ── Scenario 2: 下跌趋势 + C类过滤 ───────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario2_downtrend_filter():
    scanner = PatternScanner()
    df = _make_df(200, 'down')
    results = await scanner.scan_latest(df, 'SHITUSDT', '4h')
    assert isinstance(results, list)
    # 如果命中过滤器，则结果全为过滤命中
    filter_hits = [r for r in results if r.is_filter_hit]
    non_filter  = [r for r in results if not r.is_filter_hit]
    # 要么都是过滤命中（没有混合），要么没有过滤
    if filter_hits:
        assert len(non_filter) == 0, 'Filter and pattern results should not mix'


# ── Scenario 3: 高波动率 → 空返回 ─────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario3_high_vol_skip():
    scanner = PatternScanner()
    rng = np.random.default_rng(99)
    n = 200
    ts = pd.date_range('2024-01-01', periods=n, freq='4h')
    close = 100 + np.cumsum(rng.normal(0, 20, n))
    close = np.maximum(close, 1.0)
    df = pd.DataFrame({
        'open': close * 0.98, 'high': close * 1.6,
        'low': close * 0.4,   'close': close,
        'volume': rng.uniform(1e7, 5e7, n),
    }, index=ts)
    results = await scanner.scan_latest(df, 'EXTREMEUSDT', '4h')
    assert results == []


# ── Scenario 4: 数据不足 → 异常 ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario4_insufficient_data():
    scanner = PatternScanner()
    df = _make_df(15, 'up')
    with pytest.raises(InsufficientDataError):
        await scanner.scan_latest(df, 'TEST', '4h')


# ── Scenario 5: 所有形态定义有效性 ────────────────────────────────────────────

def test_scenario5_pattern_definitions_valid():
    valid_operators = {
        '>', '<', '>=', '<=', '==', '!=', 'between',
        'ratio_gt', 'ratio_lt', 'pct_above',
        'cross_above', 'cross_below',
        'count_gte', 'slope_positive', 'slope_negative',
        'all_below', 'all_above', 'bool_true',
    }
    valid_categories = {'A', 'B', 'C'}
    valid_directions = {'long', 'short', 'neutral'}
    valid_field_types = {'confirm', 'exclude', 'trigger', 'meta'}

    for p in ALL_PATTERNS:
        assert p.pattern_id, f'pattern_id missing: {p}'
        assert p.category in valid_categories, f'{p.pattern_id}: invalid category'
        assert p.direction in valid_directions, f'{p.pattern_id}: invalid direction'
        assert p.min_bars >= 1, f'{p.pattern_id}: min_bars too small'
        if p.category in ('A', 'B'):
            assert 0 < p.score_pass <= 100, f'{p.pattern_id}: invalid score_pass'
        assert len(p.fields) > 0, f'{p.pattern_id}: no fields'

        for f in p.fields:
            assert f.field_type in valid_field_types, \
                f'{p.pattern_id}/{f.field_id}: invalid field_type'
            assert f.operator in valid_operators, \
                f'{p.pattern_id}/{f.field_id}: invalid operator {f.operator}'
            assert f.weight >= 0, f'{p.pattern_id}/{f.field_id}: negative weight'
            assert f.penalty >= 0, f'{p.pattern_id}/{f.field_id}: negative penalty'


# ── Scenario 6: 体制一致性 ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario6_regime_consistency():
    scanner = PatternScanner()
    df = _make_df(300, 'up')

    results = await scanner.scan_latest(df, 'TEST', '4h')

    rd = MarketRegimeDetector()
    regime_result = rd.detect(df)

    for r in results:
        if r.is_filter_hit:
            continue
        # 结果中的 regime 应该与检测到的体制一致
        assert r.regime == regime_result.regime.value, \
            f'Regime mismatch: result={r.regime}, detected={regime_result.regime.value}'


# ── Scenario 7: 并发安全 ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario7_concurrent_scan():
    """多个 symbol 并发扫描，结果互不干扰"""
    symbols = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT']
    dfs = {s: _make_df(200, 'up', seed=i) for i, s in enumerate(symbols)}

    async def _scan(sym):
        scanner = PatternScanner()  # 每个任务独立 scanner
        return await scanner.scan_latest(dfs[sym], sym, '4h')

    tasks = [_scan(s) for s in symbols]
    all_results = await asyncio.gather(*tasks)

    assert len(all_results) == len(symbols)
    for sym, results in zip(symbols, all_results):
        for r in results:
            assert r.symbol == sym, f'Symbol mismatch: expected {sym}, got {r.symbol}'


# ── Scenario 8: 得分归一化 ────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_scenario8_score_normalization():
    """所有返回结果的 total_score 必须在 [0, 100] 范围内"""
    scanner = PatternScanner()
    test_cases = [
        ('UP', _make_df(200, 'up')),
        ('DOWN', _make_df(200, 'down')),
        ('FLAT', _make_df(200, 'flat')),
    ]

    for label, df in test_cases:
        results = await scanner.scan_latest(df, f'{label}USDT', '4h')
        for r in results:
            assert 0.0 <= r.total_score <= 100.0, \
                f'{label}: total_score={r.total_score} out of range for {r.pattern_id}'
            assert 0.0 <= r.regime_score <= 100.0, \
                f'{label}: regime_score={r.regime_score} out of range'
