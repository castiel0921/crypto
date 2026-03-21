"""
MarketRegimeDetector — 第0层市场体制判断（Section 09）

接口契约（Claude Code 不修改此实现）：
    detector.detect(df: pd.DataFrame) -> RegimeResult
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from .exceptions import RegimeDetectorError
from .models import Regime, RegimeResult

logger = logging.getLogger(__name__)

REGIME_SCORE_WEIGHTS = {
    'bull_trend': {'trend': 0.50, 'structure': 0.25, 'volume': 0.15, 'vol': 0.10},
    'ranging':    {'trend': 0.20, 'structure': 0.50, 'volume': 0.15, 'vol': 0.15},
    'bear_trend': {'trend': 0.50, 'structure': 0.25, 'volume': 0.15, 'vol': 0.10},
}

HIGH_VOL_ATR_RATIO = 2.0
HIGH_VOL_BB_WIDTH  = 0.15


class MarketRegimeDetector:
    """
    四种体制：bull_trend / ranging / bear_trend / high_vol
    high_vol: 触发后直接过滤，不进形态识别
    """

    def detect(self, df: pd.DataFrame) -> RegimeResult:
        if len(df) < 30:
            raise RegimeDetectorError(f'Insufficient data: {len(df)} bars, need ≥30')
        try:
            return self._detect(df)
        except RegimeDetectorError:
            raise
        except Exception as e:
            raise RegimeDetectorError(f'Regime detection failed: {e}') from e

    def _detect(self, df: pd.DataFrame) -> RegimeResult:
        # ── 基础指标 ──────────────────────────────────────────────────────────
        close  = df['close']
        high   = df['high']
        low    = df['low']
        volume = df['volume']

        ma20  = close.ewm(span=20,  adjust=False).mean()
        ma60  = close.ewm(span=60,  adjust=False).mean()
        ma120 = close.ewm(span=120, adjust=False).mean()

        # ATR
        pc = close.shift(1)
        tr = pd.concat(
            [high - low, (high - pc).abs(), (low - pc).abs()], axis=1
        ).max(axis=1)
        atr14  = tr.ewm(span=14, adjust=False).mean()
        atr_ma30 = atr14.rolling(30).mean()
        atr_ratio = float(atr14.iloc[-1] / atr_ma30.iloc[-1]) if float(atr_ma30.iloc[-1]) > 0 else 1.0

        # Bollinger Band width
        mid  = close.rolling(20).mean()
        std  = close.rolling(20).std()
        bb_w = float((std.iloc[-1] * 4) / mid.iloc[-1]) if float(mid.iloc[-1]) > 0 else 0.0

        meta: dict = {
            'atr_ratio':  round(atr_ratio, 4),
            'bb_width':   round(bb_w, 4),
        }

        # ── HIGH_VOL 检测（最高优先级）────────────────────────────────────────
        if atr_ratio > HIGH_VOL_ATR_RATIO or bb_w > HIGH_VOL_BB_WIDTH:
            meta.update(self._compute_meta(df, ma20, ma60, ma120, atr14, atr_ma30, volume))
            return RegimeResult(
                regime=Regime.HIGH_VOL,
                score=100.0,
                trend_score=0.0,
                vol_score=100.0,
                volume_score=0.0,
                btc_score=0.0,
                meta=meta,
            )

        # ── 趋势得分 ───────────────────────────────────────────────────────────
        trend_score = self._trend_score(df, ma20, ma60, ma120, close)

        # ── 结构得分 ───────────────────────────────────────────────────────────
        structure_score = self._structure_score(df, ma20, ma60, ma120, close)

        # ── 成交量得分 ─────────────────────────────────────────────────────────
        volume_score = self._volume_score(df, volume, close)

        # ── 波动率得分（越低越好，用于 ranging 体制）──────────────────────────
        vol_score = self._vol_score(atr_ratio, bb_w)

        # ── 体制分类 ───────────────────────────────────────────────────────────
        regime = self._classify(trend_score, structure_score, vol_score, ma20, ma60, ma120)

        weights = REGIME_SCORE_WEIGHTS[regime.value]
        composite = (
            trend_score     * weights['trend']
            + structure_score * weights['structure']
            + volume_score    * weights['volume']
            + vol_score       * weights['vol']
        )
        composite = max(0.0, min(100.0, composite))

        meta.update(self._compute_meta(df, ma20, ma60, ma120, atr14, atr_ma30, volume))
        meta['regime_strength'] = self._compute_regime_strength(
            regime, trend_score, composite, close, ma20, ma60, ma120, atr_ratio
        )

        return RegimeResult(
            regime       = regime,
            score        = round(composite, 2),
            trend_score  = round(trend_score, 2),
            vol_score    = round(vol_score, 2),
            volume_score = round(volume_score, 2),
            btc_score    = 50.0,   # btc_score 由外层传入，此处占位
            meta         = meta,
        )

    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _trend_score(df, ma20, ma60, ma120, close) -> float:
        v20  = float(ma20.iloc[-1])
        v60  = float(ma60.iloc[-1])
        v120 = float(ma120.iloc[-1])

        # MA 排列方向
        bull_align = v20 > v60 > v120
        bear_align = v20 < v60 < v120

        # 斜率
        def slope(series, n=10):
            if len(series) < n + 1:
                return 0.0
            prev = float(series.iloc[-(n + 1)])
            curr = float(series.iloc[-1])
            return (curr - prev) / abs(prev) if prev != 0 else 0.0

        s20  = slope(ma20)
        s60  = slope(ma60)
        s120 = slope(ma120)

        if bull_align and s20 > 0 and s60 > 0:
            base = 75.0
            bonus = min(25.0, abs(s120) * 500)
            return base + bonus
        elif bear_align and s20 < 0 and s60 < 0:
            base = 75.0
            bonus = min(25.0, abs(s120) * 500)
            return base + bonus
        elif s120 > 0 and not bear_align:
            return 50.0 + min(20.0, abs(s20) * 300)
        elif s120 < 0 and not bull_align:
            return 50.0 + min(20.0, abs(s20) * 300)
        else:
            return 20.0

    @staticmethod
    def _structure_score(df, ma20, ma60, ma120, close) -> float:
        """基于价格在均线附近的整理程度打分"""
        window = 30
        if len(df) < window:
            return 50.0
        recent_close = close.tail(window)
        m20_tail = ma20.tail(window)

        # 与 MA20 的偏离程度
        deviation = ((recent_close - m20_tail) / m20_tail.abs()).abs().mean()

        # 偏离越小（横盘）→ structure score 越高
        if deviation < 0.01:
            return 85.0
        elif deviation < 0.03:
            return 70.0
        elif deviation < 0.06:
            return 50.0
        elif deviation < 0.10:
            return 35.0
        else:
            return 15.0

    @staticmethod
    def _volume_score(df, volume, close) -> float:
        if len(volume) < 20:
            return 50.0
        vm20  = float(volume.tail(20).mean())
        vm60  = float(volume.tail(60).mean()) if len(volume) >= 60 else vm20
        ratio = vm20 / vm60 if vm60 > 0 else 1.0
        # 成交量放大（活跃）→ 高分
        if ratio > 1.5:
            return 80.0
        elif ratio > 1.2:
            return 65.0
        elif ratio > 0.8:
            return 50.0
        else:
            return 30.0

    @staticmethod
    def _vol_score(atr_ratio: float, bb_w: float) -> float:
        """ATR/BB波动率越低 → ranging 信号越强 → vol_score 越高"""
        if atr_ratio < 0.7 and bb_w < 0.05:
            return 85.0
        elif atr_ratio < 1.0 and bb_w < 0.08:
            return 70.0
        elif atr_ratio < 1.4:
            return 50.0
        elif atr_ratio < 1.8:
            return 30.0
        else:
            return 15.0

    @staticmethod
    def _classify(trend_score, structure_score, vol_score, ma20, ma60, ma120) -> Regime:
        v20  = float(ma20.iloc[-1])
        v60  = float(ma60.iloc[-1])
        v120 = float(ma120.iloc[-1])

        bull_align = v20 > v60 > v120
        bear_align = v20 < v60 < v120

        if bull_align and trend_score >= 65:
            return Regime.BULL_TREND
        elif bear_align and trend_score >= 65:
            return Regime.BEAR_TREND
        elif structure_score >= 60 and vol_score >= 50:
            return Regime.RANGING
        elif trend_score >= 55:
            return Regime.BULL_TREND if bull_align else (Regime.BEAR_TREND if bear_align else Regime.RANGING)
        else:
            return Regime.RANGING

    @staticmethod
    def _compute_regime_strength(regime, trend_score, composite, close, ma20, ma60, ma120, atr_ratio) -> str:
        """
        判断当前体制的强弱：
        - strong:  趋势得分高、价格在关键均线之上、ATR 正常
        - medium:  条件部分满足
        - weak:    趋势得分低、价格跌破关键均线
        """
        v20  = float(ma20.iloc[-1])
        v60  = float(ma60.iloc[-1])
        v120 = float(ma120.iloc[-1])
        price = float(close.iloc[-1])

        if regime.value == 'bull_trend':
            above_all = price > v20 > v60 > v120
            above_ma60 = price > v60
            if above_all and trend_score >= 80 and atr_ratio < 1.3:
                return 'strong'
            elif above_ma60 and trend_score >= 65:
                return 'medium'
            else:
                return 'weak'
        elif regime.value == 'bear_trend':
            below_all = price < v20 < v60 < v120
            below_ma60 = price < v60
            if below_all and trend_score >= 80 and atr_ratio < 1.3:
                return 'strong'
            elif below_ma60 and trend_score >= 65:
                return 'medium'
            else:
                return 'weak'
        elif regime.value == 'ranging':
            if composite >= 65 and atr_ratio < 1.0:
                return 'strong'
            elif composite >= 50:
                return 'medium'
            else:
                return 'weak'
        else:  # high_vol
            return 'weak'

    @staticmethod
    def _compute_meta(df, ma20, ma60, ma120, atr14, atr_ma30, volume) -> dict:
        close = df['close']

        def slope_10(s):
            if len(s) < 11:
                return 0.0
            prev = float(s.iloc[-11])
            curr = float(s.iloc[-1])
            return (curr - prev) / abs(prev) if prev != 0 else 0.0

        v20  = float(ma20.iloc[-1])
        v60  = float(ma60.iloc[-1])
        v120 = float(ma120.iloc[-1])

        close_tail30 = close.tail(30)
        m120_tail30  = ma120.tail(30)
        above_ratio  = float((close_tail30 > m120_tail30).mean())

        atr_r = float(atr14.iloc[-1] / atr_ma30.iloc[-1]) if float(atr_ma30.iloc[-1]) > 0 else 1.0
        mid   = close.rolling(20).mean()
        std20 = close.rolling(20).std()
        bb_w  = float((std20.iloc[-1] * 4) / float(mid.iloc[-1])) if float(mid.iloc[-1]) > 0 else 0.0

        vm20 = float(volume.tail(20).mean())
        vm_std = float(volume.tail(20).std())
        vol_cv = vm_std / vm20 if vm20 > 0 else 0.0

        return {
            'ma_bull_align':               v20 > v60 > v120,
            'ma_bear_align':               v20 < v60 < v120,
            'slope_slow_10':               round(slope_10(ma120), 6),
            'slope_fast_10':               round(slope_10(ma20),  6),
            'close_above_ma_slow_ratio30': round(above_ratio, 4),
            'atr_ratio':                   round(atr_r, 4),
            'bb_width':                    round(bb_w, 4),
            'vol_cv_20':                   round(vol_cv, 4),
        }
