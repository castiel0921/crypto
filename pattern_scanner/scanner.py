"""
PatternScanner — Section 05
核心扫描引擎，负责形态识别和评分
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from .exceptions import InsufficientDataError, MissingColumnError
from .field_evaluator import FieldEvaluator
from .indicators import IndicatorLibrary
from .models import (
    PatternDefinition,
    PatternField,
    PatternScanResult,
    Regime,
    RegimeResult,
)
from .patterns.definitions import ALL_PATTERNS
from .regime_detector import MarketRegimeDetector

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {'open', 'high', 'low', 'close', 'volume'}


def _validate_df(df: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise MissingColumnError(f'Missing columns: {missing}')
    if len(df) < 30:
        raise InsufficientDataError(f'Insufficient bars: {len(df)}, need ≥30')


class PatternScanner:
    """
    主扫描引擎。
    每次scan_latest()内部创建局部IndicatorLibrary实例，确保并发安全。
    """

    def __init__(
        self,
        patterns: Optional[list[PatternDefinition]] = None,
        btc_df: Optional[pd.DataFrame] = None,
    ):
        self.patterns = patterns if patterns is not None else ALL_PATTERNS
        self._btc_df = btc_df
        self.regime_detector = MarketRegimeDetector()

    def set_btc_df(self, btc_df: Optional[pd.DataFrame]) -> None:
        """更新BTC参照数据（由上层在每个批次开始时调用）"""
        self._btc_df = btc_df

    # ──────────────────────────────────────────────────────────────────────────
    # 主入口
    # ──────────────────────────────────────────────────────────────────────────

    async def scan_latest(
        self,
        df: pd.DataFrame,
        symbol: str,
        tf: str,
    ) -> list[PatternScanResult]:
        """
        完整扫描流程（含体制检测、过滤器、形态识别）。
        每次调用创建独立的IndicatorLibrary，并发安全。
        """
        _validate_df(df)
        regime_result = self.regime_detector.detect(df)

        if regime_result.regime == Regime.HIGH_VOL:
            logger.debug('%s/%s: HIGH_VOL skip', symbol, tf)
            return []

        local_ind = IndicatorLibrary(btc_df=self._btc_df)
        return self.scan_latest_with(df, symbol, tf, local_ind, regime_result)

    def scan_latest_with(
        self,
        df: pd.DataFrame,
        symbol: str,
        tf: str,
        local_ind: IndicatorLibrary,
        regime_result: Optional[RegimeResult] = None,
    ) -> list[PatternScanResult]:
        """
        使用外部传入的IndicatorLibrary进行扫描（供测试和批处理使用）。
        """
        _validate_df(df)

        if regime_result is None:
            regime_result = self.regime_detector.detect(df)
            if regime_result.regime == Regime.HIGH_VOL:
                return []

        evaluator = FieldEvaluator(local_ind)

        # C1/C2 过滤器
        filter_hits = self._check_filters(df, regime_result, evaluator, symbol, tf)
        if filter_hits:
            return filter_hits

        # 找出当前体制下适用的形态（A/B类）
        active = [
            p for p in self.patterns
            if p.category in ('A', 'B')
            and regime_result.regime.value in p.regime_filter
        ]

        results: list[PatternScanResult] = []
        for pattern in active:
            if len(df) < pattern.min_bars:
                continue
            r = self.score_pattern(df, pattern, regime_result, evaluator, symbol, tf)
            if r.total_score >= pattern.score_pass:
                results.append(r)

        return sorted(results, key=lambda r: r.total_score, reverse=True)

    # ──────────────────────────────────────────────────────────────────────────
    # 评分核心
    # ──────────────────────────────────────────────────────────────────────────

    def score_pattern(
        self,
        df: pd.DataFrame,
        pattern: PatternDefinition,
        regime_result: RegimeResult,
        evaluator: FieldEvaluator,
        symbol: str = '',
        tf: str = '',
    ) -> PatternScanResult:
        """
        评估单个形态的所有字段，计算综合得分。

        得分逻辑：
        - confirm 字段: 每个 required=True 的字段，命中 +weight，未命中 -penalty
        - confirm 字段: required=False 的字段，命中 +weight（加分项）
        - exclude 字段: 命中则扣分 -penalty
        - trigger 字段: 不影响得分，但记录是否触发
        """
        field_results: dict[str, bool] = {}
        raw_values:    dict[str, float] = {}

        confirm_score  = 0.0
        exclude_penalty = 0.0
        trigger_met    = False
        trigger_type:  Optional[str] = None

        confirm_fields  = [f for f in pattern.fields if f.field_type == 'confirm']
        exclude_fields  = [f for f in pattern.fields if f.field_type == 'exclude']
        trigger_fields  = [f for f in pattern.fields if f.field_type == 'trigger']

        # 满分基准：所有 required confirm 字段权重之和
        max_confirm = sum(f.weight for f in confirm_fields if f.is_required)
        if max_confirm == 0:
            max_confirm = 1.0

        # ── confirm 字段 ───────────────────────────────────────────────────────
        for f in confirm_fields:
            try:
                hit, raw = evaluator.evaluate(df, f)
            except Exception as e:
                logger.debug('Field eval error %s/%s: %s', pattern.pattern_id, f.field_id, e)
                hit, raw = False, 0.0

            field_results[f.field_id] = hit
            raw_values[f.field_id]    = raw

            if hit:
                confirm_score += f.weight
            elif f.is_required:
                confirm_score -= f.penalty

        # ── exclude 字段 ──────────────────────────────────────────────────────
        for f in exclude_fields:
            try:
                hit, raw = evaluator.evaluate(df, f)
            except Exception as e:
                logger.debug('Field eval error %s/%s: %s', pattern.pattern_id, f.field_id, e)
                hit, raw = False, 0.0

            field_results[f.field_id] = hit
            raw_values[f.field_id]    = raw

            if hit:
                exclude_penalty += f.penalty

        # ── trigger 字段 ──────────────────────────────────────────────────────
        for f in trigger_fields:
            try:
                hit, raw = evaluator.evaluate(df, f)
            except Exception as e:
                logger.debug('Field eval error %s/%s: %s', pattern.pattern_id, f.field_id, e)
                hit, raw = False, 0.0

            field_results[f.field_id] = hit
            raw_values[f.field_id]    = raw

            if hit and not trigger_met:
                trigger_met  = True
                trigger_type = f.field_name

        # ── 综合得分 ──────────────────────────────────────────────────────────
        # 归一化到 [0, 100]
        raw_score  = (confirm_score / max_confirm) * 100.0
        total_score = max(0.0, min(100.0, raw_score - exclude_penalty))

        bar_time = _get_bar_time(df)

        return PatternScanResult(
            symbol          = symbol,
            timeframe       = tf,
            bar_time        = bar_time,
            pattern_id      = pattern.pattern_id,
            pattern_name    = pattern.pattern_name,
            direction       = pattern.direction,
            regime          = regime_result.regime.value,
            regime_score    = regime_result.score,
            total_score     = round(total_score, 2),
            confirm_score   = round(confirm_score, 4),
            exclude_penalty = round(exclude_penalty, 4),
            field_results   = field_results,
            raw_values      = raw_values,
            trigger_met     = trigger_met,
            trigger_type    = trigger_type,
            rule_version    = pattern.version,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # 过滤器（C1/C2）
    # ──────────────────────────────────────────────────────────────────────────

    def _check_filters(
        self,
        df: pd.DataFrame,
        regime_result: RegimeResult,
        evaluator: FieldEvaluator,
        symbol: str = '',
        tf: str = '',
    ) -> list[PatternScanResult]:
        """
        评估 C 类过滤形态（C1/C2）。
        任意一个过滤形态命中（字段命中数 >= filter_min），则返回过滤结果，终止后续识别。
        """
        filter_patterns = [p for p in self.patterns if p.category == 'C']
        hits: list[PatternScanResult] = []

        for pattern in filter_patterns:
            if len(df) < pattern.min_bars:
                continue

            field_results: dict[str, bool] = {}
            raw_values:    dict[str, float] = {}

            hit_count = 0
            for f in pattern.fields:
                try:
                    fhit, raw = evaluator.evaluate(df, f)
                except Exception:
                    fhit, raw = False, 0.0
                field_results[f.field_id] = fhit
                raw_values[f.field_id]    = raw
                if fhit:
                    hit_count += 1

            if hit_count >= pattern.filter_min:
                bar_time = _get_bar_time(df)
                result = PatternScanResult(
                    symbol          = symbol,
                    timeframe       = tf,
                    bar_time        = bar_time,
                    pattern_id      = pattern.pattern_id,
                    pattern_name    = pattern.pattern_name,
                    direction       = pattern.direction,
                    regime          = regime_result.regime.value,
                    regime_score    = regime_result.score,
                    total_score     = 0.0,
                    confirm_score   = 0.0,
                    exclude_penalty = 0.0,
                    field_results   = field_results,
                    raw_values      = raw_values,
                    trigger_met     = False,
                    is_filter_hit   = True,
                    rule_version    = pattern.version,
                )
                hits.append(result)

        return hits

    # ──────────────────────────────────────────────────────────────────────────
    # 时序扫描（回测用）
    # ──────────────────────────────────────────────────────────────────────────

    def scan_series(
        self,
        df: pd.DataFrame,
        symbol: str,
        tf: str,
        pattern_ids: Optional[list[str]] = None,
        min_score: float = 60.0,
        step: int = 1,
    ) -> pd.DataFrame:
        """
        滚动窗口扫描，返回每个时间点的扫描结果（回测用）。
        结果以 DataFrame 形式返回，列名格式：rv__{field_id}, fr__{field_id}
        """
        _validate_df(df)
        target_patterns = self.patterns
        if pattern_ids:
            target_patterns = [p for p in self.patterns if p.pattern_id in pattern_ids]

        records = []
        min_window = 120  # 至少需要120根K线

        for i in range(min_window, len(df) + 1, step):
            window_df = df.iloc[:i].copy()
            try:
                regime_result = self.regime_detector.detect(window_df)
                if regime_result.regime == Regime.HIGH_VOL:
                    continue

                local_ind = IndicatorLibrary(btc_df=self._btc_df)
                evaluator = FieldEvaluator(local_ind)

                for pattern in target_patterns:
                    if len(window_df) < pattern.min_bars:
                        continue
                    if pattern.category == 'C':
                        continue

                    r = self.score_pattern(
                        window_df, pattern, regime_result, evaluator, symbol, tf
                    )
                    if r.total_score < min_score:
                        continue

                    row = {
                        'bar_time':    r.bar_time,
                        'symbol':      symbol,
                        'timeframe':   tf,
                        'pattern_id':  r.pattern_id,
                        'regime':      r.regime,
                        'total_score': r.total_score,
                        'trigger_met': r.trigger_met,
                    }
                    # rv__ = raw values, fr__ = field results
                    for fid, val in r.raw_values.items():
                        row[f'rv__{fid}'] = val
                    for fid, hit in r.field_results.items():
                        row[f'fr__{fid}'] = hit

                    records.append(row)

            except Exception as e:
                logger.debug('scan_series error at i=%d: %s', i, e)
                continue

        if not records:
            return pd.DataFrame()
        return pd.DataFrame(records)

    # ──────────────────────────────────────────────────────────────────────────
    # 候选过滤
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def get_candidates(
        results: list[PatternScanResult],
        min_score: float = 70.0,
        trigger_only: bool = False,
        exclude_filters: bool = True,
    ) -> list[PatternScanResult]:
        """
        从扫描结果中筛选高质量候选。
        """
        out = []
        for r in results:
            if exclude_filters and r.is_filter_hit:
                continue
            if r.total_score < min_score:
                continue
            if trigger_only and not r.trigger_met:
                continue
            out.append(r)
        return sorted(out, key=lambda r: r.total_score, reverse=True)


# ──────────────────────────────────────────────────────────────────────────────

def _get_bar_time(df: pd.DataFrame) -> datetime:
    """取最后一根K线的时间戳"""
    idx = df.index[-1]
    if isinstance(idx, (pd.Timestamp, datetime)):
        return pd.Timestamp(idx).to_pydatetime()
    # 尝试从 'open_time' 或 'timestamp' 列读取
    for col in ('open_time', 'timestamp', 'time'):
        if col in df.columns:
            v = df[col].iloc[-1]
            if isinstance(v, (int, float)):
                return datetime.fromtimestamp(v / 1000)
            return pd.Timestamp(v).to_pydatetime()
    return datetime.utcnow()
