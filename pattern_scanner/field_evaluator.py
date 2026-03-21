"""
FieldEvaluator — 14种 operator 完整规范（Section 04）
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .indicators import IndicatorLibrary
from .models import PatternField

logger = logging.getLogger(__name__)


class FieldEvaluator:
    def __init__(self, indicators: IndicatorLibrary):
        self.ind = indicators

    def evaluate(self, df: pd.DataFrame, field: PatternField) -> tuple[bool, float]:
        """
        返回 (hit: bool, raw: float)
        raw 是用于调试/LLM的原始指标值
        """
        value   = self._get_value(df, field.indicator)
        ref_val = self._get_ref(df, field) if field.ref_indicator else None

        hit = self._apply_operator(df, value, ref_val, field)
        raw = self._compute_raw(value, ref_val, field)
        return bool(hit), float(raw) if raw is not None and not np.isnan(float(raw)) else 0.0

    def _get_value(self, df: pd.DataFrame, name: str):
        series = self.ind.compute(df, name)
        return series.iloc[-1]

    def _get_ref(self, df: pd.DataFrame, field: PatternField) -> float:
        series = self.ind.compute(df, field.ref_indicator)
        return float(series.iloc[-1]) * field.ref_multiplier

    def _compute_raw(self, value, ref_val, field: PatternField) -> Optional[float]:
        try:
            v = float(value)
            if field.operator in ('ratio_gt', 'ratio_lt') and ref_val is not None and ref_val != 0:
                return v / ref_val
            if field.operator == 'pct_above' and ref_val is not None and ref_val != 0:
                return (v - ref_val) / abs(ref_val)
            return v
        except Exception:
            return 0.0

    def _apply_operator(
        self,
        df: pd.DataFrame,
        value,
        ref_val: Optional[float],
        field: PatternField,
    ) -> bool:
        op = field.operator
        v  = float(value) if not isinstance(value, bool) else float(value)
        a  = field.param_a
        b  = field.param_b

        # ── 简单比较 ──────────────────────────────────────────────────────────
        if op == '>':
            return v > a
        if op == '<':
            return v < a
        if op == '>=':
            return v >= a
        if op == '<=':
            return v <= a
        if op == '==':
            return abs(v - a) < 1e-9
        if op == '!=':
            return abs(v - a) >= 1e-9

        # ── between ──────────────────────────────────────────────────────────
        if op == 'between':
            return a <= v <= (b if b is not None else a)

        # ── ratio_gt / ratio_lt ───────────────────────────────────────────────
        if op == 'ratio_gt':
            if ref_val is None or ref_val == 0:
                return v > a          # fallback: direct comparison
            return (v / ref_val) > a
        if op == 'ratio_lt':
            if ref_val is None or ref_val == 0:
                return v < a
            return (v / ref_val) < a

        # ── pct_above ────────────────────────────────────────────────────────
        if op == 'pct_above':
            if ref_val is None or ref_val == 0:
                return False
            return ((v - ref_val) / abs(ref_val)) > a

        # ── cross_above / cross_below ─────────────────────────────────────────
        if op in ('cross_above', 'cross_below'):
            series = self.ind.compute(df, field.indicator)
            if len(series) < 2:
                return False
            curr = float(series.iloc[-1])
            prev = float(series.iloc[-2])
            target = ref_val if ref_val is not None else a
            if op == 'cross_above':
                return prev < target <= curr
            else:
                return prev >= target > curr

        # ── count_gte ────────────────────────────────────────────────────────
        if op == 'count_gte':
            series = self.ind.compute(df, field.indicator)
            tail   = series.tail(field.lookback)
            count  = float(tail.sum())
            return count >= a

        # ── all_below / all_above ─────────────────────────────────────────────
        if op == 'all_below':
            series = self.ind.compute(df, field.indicator)
            tail   = series.tail(field.lookback)
            target = ref_val if ref_val is not None else a
            return bool((tail < target).all())

        if op == 'all_above':
            series = self.ind.compute(df, field.indicator)
            tail   = series.tail(field.lookback)
            target = ref_val if ref_val is not None else a
            return bool((tail > target).all())

        # ── slope_positive / slope_negative ───────────────────────────────────
        if op == 'slope_positive':
            series = self.ind.compute(df, field.indicator)
            tail   = series.tail(field.lookback)
            if len(tail) < 2:
                return False
            diffs = tail.diff().dropna()
            return bool((diffs > 0).all())

        if op == 'slope_negative':
            series = self.ind.compute(df, field.indicator)
            tail   = series.tail(field.lookback)
            if len(tail) < 2:
                return False
            diffs = tail.diff().dropna()
            return bool((diffs < 0).all())

        # ── bool_true ────────────────────────────────────────────────────────
        if op == 'bool_true':
            return bool(v > 0.5)

        logger.warning(f'Unknown operator: {op}')
        return False
