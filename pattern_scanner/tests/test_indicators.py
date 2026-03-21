"""
IndicatorLibrary 单元测试

覆盖：
- 均线类指标
- 波动率类指标
- 成交量类指标
- 布尔复合指标
- 缓存一致性
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from ..exceptions import IndicatorComputeError
from ..indicators import IndicatorLibrary
from .conftest import _make_df


@pytest.fixture
def ind():
    return IndicatorLibrary()


# ── 均线类 ─────────────────────────────────────────────────────────────────────

class TestMovingAverages:
    def test_ma20_basic(self, ind, df_up):
        s = ind.compute(df_up, 'ma20')
        assert len(s) == len(df_up)
        assert not s.isna().all()

    def test_ma20_values_reasonable(self, ind, df_up):
        s = ind.compute(df_up, 'ma20')
        last = float(s.iloc[-1])
        close_last = float(df_up['close'].iloc[-1])
        assert abs(last - close_last) / close_last < 0.3

    def test_ma60_ma120_exist(self, ind, df_up):
        s60  = ind.compute(df_up, 'ma60')
        s120 = ind.compute(df_up, 'ma120')
        assert len(s60)  == len(df_up)
        assert len(s120) == len(df_up)

    def test_bull_ma_align_returns_bool(self, ind, df_up):
        s = ind.compute(df_up, 'bull_ma_align')
        assert s.iloc[-1] in (True, False)

    def test_bear_ma_align_returns_bool(self, ind, df_down):
        s = ind.compute(df_down, 'bear_ma_align')
        assert s.iloc[-1] in (True, False)

    def test_close_above_ma20(self, ind, df_up):
        s = ind.compute(df_up, 'close_above_ma20')
        assert set(s.dropna().unique()).issubset({True, False})

    def test_ma20_slope(self, ind, df_up):
        s = ind.compute(df_up, 'ma20_slope')
        assert len(s) == len(df_up)
        # 上升趋势斜率应为正
        assert float(s.iloc[-1]) > 0


# ── 波动率类 ───────────────────────────────────────────────────────────────────

class TestVolatility:
    def test_atr14_positive(self, ind, df_up):
        s = ind.compute(df_up, 'atr14')
        assert float(s.iloc[-1]) > 0

    def test_bb_width_reasonable(self, ind, df_up):
        s = ind.compute(df_up, 'bb_width')
        assert float(s.iloc[-1]) > 0
        assert float(s.iloc[-1]) < 1.0

    def test_bar_range_pct(self, ind, df_up):
        s = ind.compute(df_up, 'bar_range_pct')
        assert (s.iloc[-10:] >= 0).all()

    def test_lower_shadow(self, ind, df_up):
        s = ind.compute(df_up, 'lower_shadow')
        assert len(s) == len(df_up)

    def test_body_size(self, ind, df_up):
        s = ind.compute(df_up, 'body_size')
        assert (s >= 0).all()


# ── 成交量类 ───────────────────────────────────────────────────────────────────

class TestVolume:
    def test_vol_ratio(self, ind, df_up):
        s = ind.compute(df_up, 'vol_ratio')
        assert float(s.iloc[-1]) > 0

    def test_vol_ma20(self, ind, df_up):
        s = ind.compute(df_up, 'vol_ma20')
        assert float(s.iloc[-1]) > 0

    def test_bear_vol_spike_bool(self, ind, df_down):
        s = ind.compute(df_down, 'bear_vol_spike')
        assert set(s.dropna().unique()).issubset({True, False})


# ── 价格结构类 ─────────────────────────────────────────────────────────────────

class TestPriceStructure:
    def test_local_highs(self, ind, df_up):
        s = ind.compute(df_up, 'local_highs')
        assert len(s) == len(df_up)

    def test_local_lows(self, ind, df_up):
        s = ind.compute(df_up, 'local_lows')
        assert len(s) == len(df_up)

    def test_support_touch_count(self, ind, df_flat):
        s = ind.compute(df_flat, 'support_touch_count')
        assert len(s) == len(df_flat)

    def test_rsi_range(self, ind, df_up):
        s = ind.compute(df_up, 'rsi14')
        valid = s.dropna()
        assert (valid >= 0).all() and (valid <= 100).all()

    def test_three_lower_highs(self, ind, df_down):
        s = ind.compute(df_down, 'three_lower_highs')
        assert len(s) == len(df_down)


# ── 缓存一致性 ─────────────────────────────────────────────────────────────────

class TestCaching:
    def test_cache_hit(self, ind, df_up):
        s1 = ind.compute(df_up, 'ma20')
        s2 = ind.compute(df_up, 'ma20')
        pd.testing.assert_series_equal(s1, s2)

    def test_different_indicators_independent(self, ind, df_up):
        s1 = ind.compute(df_up, 'ma20')
        s2 = ind.compute(df_up, 'ma60')
        last_20 = float(s1.iloc[-1])
        last_60 = float(s2.iloc[-1])
        assert last_20 != last_60

    def test_short_df_no_crash(self):
        """短序列不应崩溃"""
        ind = IndicatorLibrary()
        df = _make_df(25, 'up')
        # 短序列可能返回全NaN，但不应抛出 IndicatorComputeError 以外的异常
        try:
            s = ind.compute(df, 'ma20')
            assert len(s) == len(df)
        except IndicatorComputeError:
            pass  # 可接受

    def test_raw_price_indicators(self, ind, df_up):
        close = ind.compute(df_up, 'close')
        pd.testing.assert_series_equal(close, df_up['close'])
