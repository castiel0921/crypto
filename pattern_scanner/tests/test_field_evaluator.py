"""
FieldEvaluator 单元测试

覆盖所有14种 operator 的正向/边界/负向案例
"""
from __future__ import annotations

import pytest
import pandas as pd
import numpy as np

from ..field_evaluator import FieldEvaluator
from ..indicators import IndicatorLibrary
from ..models import PatternField


def _field(operator, param_a=0.0, param_b=None, indicator='close_above_ma20',
           ref_indicator=None, ref_multiplier=1.0, lookback=5, weight=1.0):
    return PatternField(
        field_id       = 'test_field',
        pattern_id     = 'test',
        field_name     = 'Test Field',
        field_type     = 'confirm',
        is_required    = True,
        indicator      = indicator,
        operator       = operator,
        param_a        = param_a,
        param_b        = param_b,
        lookback       = lookback,
        weight         = weight,
        ref_indicator  = ref_indicator,
        ref_multiplier = ref_multiplier,
    )


@pytest.fixture
def evaluator(df_up):
    ind = IndicatorLibrary()
    return FieldEvaluator(ind)


class TestSimpleComparisons:
    def test_gt_hit(self, evaluator, df_up):
        f = _field('>', param_a=-999.0, indicator='ma20')
        hit, raw = evaluator.evaluate(df_up, f)
        assert hit is True
        assert isinstance(raw, float)

    def test_gt_miss(self, evaluator, df_up):
        f = _field('>', param_a=999999.0, indicator='ma20')
        hit, raw = evaluator.evaluate(df_up, f)
        assert hit is False

    def test_lt_hit(self, evaluator, df_up):
        f = _field('<', param_a=999999.0, indicator='ma20')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_gte(self, evaluator, df_up):
        f = _field('>=', param_a=0.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_lte(self, evaluator, df_up):
        f = _field('<=', param_a=100.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_eq_miss(self, evaluator, df_up):
        f = _field('==', param_a=999.123456, indicator='ma20')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is False

    def test_ne_hit(self, evaluator, df_up):
        f = _field('!=', param_a=0.0, indicator='ma20')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True


class TestBetween:
    def test_between_in_range(self, evaluator, df_up):
        f = _field('between', param_a=0.0, param_b=100.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_between_out_of_range(self, evaluator, df_up):
        f = _field('between', param_a=200.0, param_b=300.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is False


class TestRatioOperators:
    def test_ratio_gt_no_ref(self, evaluator, df_up):
        f = _field('ratio_gt', param_a=0.0, indicator='vol_ratio')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_ratio_lt_no_ref(self, evaluator, df_up):
        f = _field('ratio_lt', param_a=99999.0, indicator='vol_ratio')
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_ratio_gt_with_ref(self, evaluator, df_up):
        # vol_ma20 / (vol_ma20 * 0.5) = 2.0 > 0 → True
        f = _field('ratio_gt', param_a=0.0,
                   indicator='vol_ma20', ref_indicator='vol_ma20', ref_multiplier=0.5)
        hit, raw = evaluator.evaluate(df_up, f)
        assert hit is True
        assert abs(raw - 2.0) < 0.01


class TestPctAbove:
    def test_pct_above_hit(self, evaluator, df_up):
        f = _field('pct_above', param_a=-1.0,
                   indicator='ma20', ref_indicator='ma60', ref_multiplier=1.0)
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_pct_above_miss(self, evaluator, df_up):
        f = _field('pct_above', param_a=10.0,
                   indicator='ma20', ref_indicator='ma60', ref_multiplier=1.0)
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is False


class TestCrossOperators:
    def test_cross_above_returns_bool(self, evaluator, df_up):
        f = _field('cross_above', param_a=50.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert isinstance(hit, bool)

    def test_cross_below_returns_bool(self, evaluator, df_up):
        f = _field('cross_below', param_a=50.0, indicator='rsi14')
        hit, _ = evaluator.evaluate(df_up, f)
        assert isinstance(hit, bool)


class TestCountGte:
    def test_count_gte_bool_series(self, evaluator, df_up):
        f = _field('count_gte', param_a=1.0, indicator='close_above_ma20', lookback=20)
        hit, _ = evaluator.evaluate(df_up, f)
        assert isinstance(hit, bool)


class TestAllAboveBelow:
    def test_all_above_hit(self, evaluator, df_up):
        f = _field('all_above', param_a=0.0, indicator='ma20', lookback=5)
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is True

    def test_all_below_miss(self, evaluator, df_up):
        f = _field('all_below', param_a=-1.0, indicator='ma20', lookback=5)
        hit, _ = evaluator.evaluate(df_up, f)
        assert hit is False


class TestSlopeOperators:
    def test_slope_positive_uptrend(self, evaluator, df_up):
        f = _field('slope_positive', indicator='ma20', lookback=3)
        hit, _ = evaluator.evaluate(df_up, f)
        assert isinstance(hit, bool)

    def test_slope_negative_downtrend(self, evaluator, df_down):
        ind = IndicatorLibrary()
        ev = FieldEvaluator(ind)
        f = _field('slope_negative', indicator='ma20', lookback=3)
        hit, _ = ev.evaluate(df_down, f)
        assert isinstance(hit, bool)


class TestBoolTrue:
    def test_bool_true_with_indicator(self, evaluator, df_up):
        f = _field('bool_true', indicator='bull_ma_align')
        hit, _ = evaluator.evaluate(df_up, f)
        assert isinstance(hit, bool)

    def test_raw_value_not_nan(self, evaluator, df_up):
        f = _field('>', param_a=0.0, indicator='ma20')
        hit, raw = evaluator.evaluate(df_up, f)
        assert raw == raw  # not NaN
