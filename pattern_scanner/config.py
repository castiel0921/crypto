from __future__ import annotations
import copy
from typing import Optional

from .models import PatternDefinition

DEFAULT_CONFIG: dict = {
    # 均线周期
    'ma_fast':   20,
    'ma_mid':    60,
    'ma_slow':   120,
    # 指标周期
    'atr_period':    14,
    'rsi_period':    14,
    'bb_period':     20,
    'bb_std':        2.0,
    'vol_ma_period': 20,
    # 局部极值
    'local_high_order': 3,
    'local_low_order':  3,
    # 体制权重
    'regime_weights.bull_trend.trend':     0.50,
    'regime_weights.bull_trend.structure': 0.25,
    'regime_weights.bull_trend.volume':    0.15,
    'regime_weights.bull_trend.vol':       0.10,
    'regime_weights.ranging.trend':        0.20,
    'regime_weights.ranging.structure':    0.50,
    'regime_weights.ranging.volume':       0.15,
    'regime_weights.ranging.vol':          0.15,
    'regime_weights.bear_trend.trend':     0.50,
    'regime_weights.bear_trend.structure': 0.25,
    'regime_weights.bear_trend.volume':    0.15,
    'regime_weights.bear_trend.vol':       0.10,
    # 扫描行为
    'scan_series_step': 1,
    'min_df_length':    150,
    # HIGH_VOL 阈值
    'high_vol_atr_ratio': 2.0,
    'high_vol_bb_width':  0.15,
}

REGIME_SCORE_WEIGHTS: dict = {
    'bull_trend': {'trend': 0.50, 'structure': 0.25, 'volume': 0.15, 'vol': 0.10},
    'ranging':    {'trend': 0.20, 'structure': 0.50, 'volume': 0.15, 'vol': 0.15},
    'bear_trend': {'trend': 0.50, 'structure': 0.25, 'volume': 0.15, 'vol': 0.10},
    'high_vol':   None,
}


def build_config(overrides: Optional[dict] = None) -> dict:
    return {**DEFAULT_CONFIG, **(overrides or {})}


def apply_config_to_patterns(
    patterns: list[PatternDefinition],
    config: dict,
) -> list[PatternDefinition]:
    """按 "PID.FID.attr": value 格式覆盖对应 PatternField 的阈值"""
    patterns = copy.deepcopy(patterns)
    for key, val in config.items():
        parts = key.split('.')
        if len(parts) == 3:
            pid, fid, attr = parts
            for p in patterns:
                if p.pattern_id == pid:
                    for f in p.fields:
                        if f.field_id == fid:
                            setattr(f, attr, val)
    return patterns
