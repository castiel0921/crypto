"""
IndicatorLibrary — 所有指标计算规范（Section 03）

所有指标函数输入 OHLCV DataFrame（时间索引UTC升序），返回等长 pandas Series（头部NaN填充）。
IndicatorLibrary 每次 scan_latest 调用创建局部实例，不跨标的共享。
"""
from __future__ import annotations

import hashlib
import logging
import re
from typing import Optional

import numpy as np
import pandas as pd
from scipy.signal import argrelextrema

from .exceptions import IndicatorComputeError

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────────────
# 3.1  均线类
# ──────────────────────────────────────────────────────────────────────────────

def ma(df: pd.DataFrame, n: int) -> pd.Series:
    return df['close'].ewm(span=n, adjust=False).mean()


def ma_slope(df: pd.DataFrame, n: int, lookback: int = 10) -> pd.Series:
    m = ma(df, n)
    prev = m.shift(lookback)
    return (m - prev) / prev.abs().replace(0, np.nan)


def ma20_ma60_spread_pct(df: pd.DataFrame) -> pd.Series:
    return (ma(df, 20) - ma(df, 60)).abs() / df['close']


def close_above_ma(df: pd.DataFrame, n: int) -> pd.Series:
    return (df['close'] > ma(df, n)).astype(float)


def close_below_ma(df: pd.DataFrame, n: int) -> pd.Series:
    return (df['close'] < ma(df, n)).astype(float)


def bear_ma_align(df: pd.DataFrame) -> pd.Series:
    ma20  = ma(df, 20)
    ma60  = ma(df, 60)
    ma120 = ma(df, 120)
    s20   = ma_slope(df, 20)
    s60   = ma_slope(df, 60)
    s120  = ma_slope(df, 120)
    cond  = (ma20 < ma60) & (ma60 < ma120) & (s20 < 0) & (s60 < 0) & (s120 < 0)
    return cond.astype(float)


def bull_ma_align(df: pd.DataFrame) -> pd.Series:
    ma20  = ma(df, 20)
    ma60  = ma(df, 60)
    ma120 = ma(df, 120)
    s20   = ma_slope(df, 20)
    cond  = (ma20 > ma60) & (ma60 > ma120) & (s20 > 0)
    return cond.astype(float)


def ma_order_changes(df: pd.DataFrame, lookback: int = 10) -> pd.Series:
    """近lookback根内MA20/60/120排列顺序变化次数"""
    ma20  = ma(df, 20)
    ma60  = ma(df, 60)
    ma120 = ma(df, 120)

    def _state(i):
        if ma20.iloc[i] > ma60.iloc[i] > ma120.iloc[i]:
            return 'bull'
        elif ma20.iloc[i] < ma60.iloc[i] < ma120.iloc[i]:
            return 'bear'
        return 'mixed'

    result = pd.Series(np.nan, index=df.index)
    for end in range(lookback, len(df)):
        changes = 0
        prev = _state(end - lookback)
        for j in range(end - lookback + 1, end + 1):
            curr = _state(j)
            if curr != prev:
                changes += 1
                prev = curr
        result.iloc[end] = changes
    return result


# ──────────────────────────────────────────────────────────────────────────────
# 3.2  波动率类
# ──────────────────────────────────────────────────────────────────────────────

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    h, l, pc = df['high'], df['low'], df['close'].shift(1)
    tr = pd.concat([h - l, (h - pc).abs(), (l - pc).abs()], axis=1).max(axis=1)
    return tr.ewm(span=n, adjust=False).mean()


def atr_ma(df: pd.DataFrame, n_atr: int = 14, n_ma: int = 30) -> pd.Series:
    return atr(df, n_atr).rolling(n_ma).mean()


def bb_width(df: pd.DataFrame, period: int = 20, std_mult: float = 2.0) -> pd.Series:
    mid   = df['close'].rolling(period).mean()
    std   = df['close'].rolling(period).std()
    upper = mid + std * std_mult
    lower = mid - std * std_mult
    return (upper - lower) / mid.replace(0, np.nan)


def bar_range_pct(df: pd.DataFrame) -> pd.Series:
    return (df['high'] - df['low']) / df['close'].replace(0, np.nan)


def avg_bar_range(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    return bar_range_pct(df).rolling(lookback).mean()


def lower_shadow(df: pd.DataFrame) -> pd.Series:
    return df[['open', 'close']].min(axis=1) - df['low']


def upper_shadow(df: pd.DataFrame) -> pd.Series:
    return df['high'] - df[['open', 'close']].max(axis=1)


def body_size(df: pd.DataFrame) -> pd.Series:
    return (df['close'] - df['open']).abs()


def long_wick_count(df: pd.DataFrame, lookback: int = 10, ratio: float = 2.5) -> pd.Series:
    ls = lower_shadow(df)
    us = upper_shadow(df)
    bs = body_size(df).replace(0, 1e-10)
    max_shadow = pd.concat([ls, us], axis=1).max(axis=1)
    per_bar = (max_shadow >= bs * ratio).astype(float)
    return per_bar.rolling(lookback).sum()


# ──────────────────────────────────────────────────────────────────────────────
# 3.3  成交量类
# ──────────────────────────────────────────────────────────────────────────────

def vol_ma(df: pd.DataFrame, n: int = 20) -> pd.Series:
    return df['volume'].rolling(n).mean()


def vol_ratio(df: pd.DataFrame, n: int = 20) -> pd.Series:
    vm = vol_ma(df, n)
    return df['volume'] / vm.replace(0, np.nan)


def bear_vol_spike(df: pd.DataFrame, lookback: int = 15, mult: float = 2.0) -> pd.Series:
    """近lookback根中是否存在收阴且成交量>vol_ma20×mult"""
    vm = vol_ma(df, 20)
    per_bar = ((df['close'] < df['open']) & (df['volume'] > vm * mult)).astype(float)
    return per_bar.rolling(lookback).max()


def bull_vol_spike(df: pd.DataFrame, lookback: int = 10, mult: float = 2.0) -> pd.Series:
    vm = vol_ma(df, 20)
    per_bar = ((df['close'] > df['open']) & (df['volume'] > vm * mult)).astype(float)
    return per_bar.rolling(lookback).max()


def bull_vol_large(df: pd.DataFrame, lookback: int = 10, mult: float = 1.5) -> pd.Series:
    """近lookback根中收阳且volume > vol_ma20×mult的per-bar布尔"""
    vm = vol_ma(df, 20)
    per_bar = ((df['close'] > df['open']) & (df['volume'] > vm * mult)).astype(float)
    return per_bar


def ma60_cross_count(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    """近lookback根内close与MA60交叉次数"""
    m60 = ma(df, 60)
    above = (df['close'] > m60).astype(int)
    cross = above.diff().abs()
    return cross.rolling(lookback).sum()


def vol_ma_prev(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """前period根的移动均量（shift by period）"""
    return df['volume'].rolling(period).mean().shift(period)


# ──────────────────────────────────────────────────────────────────────────────
# 3.4  价格结构类
# ──────────────────────────────────────────────────────────────────────────────

def local_highs(df: pd.DataFrame, order: int = 3) -> pd.Series:
    result = pd.Series(0.0, index=df.index)
    if len(df) < order * 2 + 1:
        return result
    h = df['high'].values
    idx = argrelextrema(h, np.greater, order=order)[0]
    result.iloc[idx] = 1.0
    return result


def local_lows(df: pd.DataFrame, order: int = 3) -> pd.Series:
    result = pd.Series(0.0, index=df.index)
    if len(df) < order * 2 + 1:
        return result
    l = df['low'].values
    idx = argrelextrema(l, np.less, order=order)[0]
    result.iloc[idx] = 1.0
    return result


def three_lower_highs(df: pd.DataFrame, lookback: int = 30, order: int = 3) -> pd.Series:
    """近lookback根内最近3个局部高点依次降低"""
    lh = local_highs(df, order)
    result = pd.Series(0.0, index=df.index)
    for end in range(lookback, len(df)):
        window_h = df['high'].iloc[end - lookback:end + 1]
        window_lh = lh.iloc[end - lookback:end + 1]
        highs = window_h[window_lh > 0].values
        if len(highs) >= 3:
            last3 = highs[-3:]
            if last3[0] > last3[1] > last3[2]:
                result.iloc[end] = 1.0
    return result


def higher_lows_3(df: pd.DataFrame, lookback: int = 30, order: int = 3) -> pd.Series:
    ll = local_lows(df, order)
    result = pd.Series(0.0, index=df.index)
    for end in range(lookback, len(df)):
        window_l = df['low'].iloc[end - lookback:end + 1]
        window_ll = ll.iloc[end - lookback:end + 1]
        lows = window_l[window_ll > 0].values
        if len(lows) >= 3:
            last3 = lows[-3:]
            if last3[0] < last3[1] < last3[2]:
                result.iloc[end] = 1.0
    return result


def platform_low(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    return df['close'].rolling(lookback).min()


def platform_high(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    return df['close'].rolling(lookback).max()


def max_drawdown_40(df: pd.DataFrame) -> pd.Series:
    rolling_max = df['high'].rolling(40).max()
    rolling_min = df['low'].rolling(40).min()
    return (rolling_max - rolling_min) / rolling_max.replace(0, np.nan)


def support_level(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    recent_low = df['low'].rolling(lookback).min()
    m120 = ma(df, 120)
    return pd.concat([recent_low, m120], axis=1).min(axis=1)


def support_break_pct(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    support = support_level(df, lookback)
    worst = df[['low', 'close']].min(axis=1)
    return (worst - support) / support.replace(0, np.nan)


def support_touch_count(df: pd.DataFrame, lookback: int = 30, tol: float = 0.005) -> pd.Series:
    """价格进入support±tol区间后当根或次根收回上方的次数"""
    support = support_level(df, lookback)
    result = pd.Series(0.0, index=df.index)
    for end in range(lookback, len(df)):
        count = 0
        sup = support.iloc[end]
        for j in range(end - lookback, end):
            low_j  = df['low'].iloc[j]
            close_j = df['close'].iloc[j]
            if low_j <= sup * (1 + tol) and low_j >= sup * (1 - tol):
                # 触及支撑区间，检查收回
                if close_j > sup:
                    count += 1
                elif j + 1 <= end and df['close'].iloc[j + 1] > sup:
                    count += 1
        result.iloc[end] = count
    return result


def platform_start_price(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    """平台起点价格（lookback根前的收盘价）"""
    return df['close'].shift(lookback)


def max_advance_45(df: pd.DataFrame) -> pd.Series:
    """近45根最高点相对起点涨幅"""
    start_price = df['close'].shift(44)
    rolling_max = df['high'].rolling(45).max()
    return (rolling_max - start_price) / start_price.abs().replace(0, np.nan)


def platform_range_ratio(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    """近30根(最高-最低)/最低"""
    pmax = df['close'].rolling(lookback).max()
    pmin = df['close'].rolling(lookback).min()
    return (pmax - pmin) / pmin.replace(0, np.nan)


def max_decline_40(df: pd.DataFrame) -> pd.Series:
    """近40根起点到最低点跌幅"""
    start_price = df['close'].shift(39)
    rolling_min = df['low'].rolling(40).min()
    return (start_price - rolling_min) / start_price.abs().replace(0, np.nan)


def close_below_platform_series(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    """per-bar: close < platform_low"""
    plow = platform_low(df, lookback)
    return (df['close'] < plow).astype(float)


def range_bar_series(df: pd.DataFrame, lookback: int = 35) -> pd.Series:
    """per-bar: 该根振幅 <= 整体价格范围×0.35"""
    prange = (platform_high(df, lookback) - platform_low(df, lookback)) / df['close'].replace(0, np.nan)
    return (bar_range_pct(df) <= prange * 0.35).astype(float)


def rebound_vol_ratio(df: pd.DataFrame, lookback: int = 10) -> pd.Series:
    """近lookback根均量 / 前lookback根均量"""
    current_avg  = df['volume'].rolling(lookback).mean()
    previous_avg = df['volume'].rolling(lookback).mean().shift(lookback)
    return current_avg / previous_avg.replace(0, np.nan)


def prev_local_high_price(df: pd.DataFrame, order: int = 3) -> pd.Series:
    """上一个局部高点的价格"""
    lh = local_highs(df, order)
    result = pd.Series(np.nan, index=df.index)
    last_high = np.nan
    for i in range(len(df)):
        if lh.iloc[i] > 0:
            result.iloc[i] = last_high
            last_high = df['high'].iloc[i]
        else:
            result.iloc[i] = last_high
    return result


def rebound_high_price(df: pd.DataFrame, lookback: int = 10) -> pd.Series:
    """近lookback根的最高价（当前反弹高点）"""
    return df['high'].rolling(lookback).max()


def rebound_slope_ratio(df: pd.DataFrame, lookback: int = 10) -> pd.Series:
    """反弹段斜率 / 下跌段斜率绝对值"""
    rebound_slope = (df['close'] - df['close'].shift(lookback)) / lookback
    decline_slope = (df['close'].shift(lookback) - df['close'].shift(lookback * 2)) / lookback
    return rebound_slope.abs() / decline_slope.abs().replace(0, np.nan)


def recovered_above_support(df: pd.DataFrame, lookback: int = 3) -> pd.Series:
    """跌破支撑后收回（per-bar：该根close > support）"""
    support = support_level(df, 20)
    broke   = (df[['low', 'close']].min(axis=1) < support).shift(1)
    recovered = df['close'] > support
    return (broke & recovered).astype(float)


def secondary_breakdown(df: pd.DataFrame, lookback: int = 5) -> pd.Series:
    """收回后lookback根内再次有效跌破支撑"""
    support = support_level(df, 20)
    result = pd.Series(0.0, index=df.index)
    for end in range(lookback + 1, len(df)):
        # check if there was a break, then recovery, then another break
        sup = support.iloc[end]
        break_then_recover = False
        for j in range(end - lookback - 1, end - 1):
            if df['low'].iloc[j] < sup and df['close'].iloc[j] > sup:
                break_then_recover = True
            if break_then_recover and df['close'].iloc[j + 1] < sup * 0.99:
                result.iloc[end] = 1.0
                break
    return result


def triple_ma_suppress(df: pd.DataFrame) -> pd.Series:
    """MA20/MA60/MA120均在收盘上方且斜率均负"""
    m20  = ma(df, 20)
    m60  = ma(df, 60)
    m120 = ma(df, 120)
    s20  = ma_slope(df, 20)
    s60  = ma_slope(df, 60)
    s120 = ma_slope(df, 120)
    cond = (m20 > df['close']) & (m60 > df['close']) & (m120 > df['close']) & (s20 < 0) & (s60 < 0) & (s120 < 0)
    return cond.astype(float)


def close_above_ma60_sustained(df: pd.DataFrame, n: int = 2) -> pd.Series:
    """收盘站上MA60并维持n根"""
    above = close_above_ma(df, 60)
    return above.rolling(n).min()


def recent_low_above_prior(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    """近lookback根低点 > lookback根前低点"""
    recent = df['low'].rolling(lookback).min()
    prior  = df['low'].rolling(lookback).min().shift(lookback)
    return (recent > prior).astype(float)


def new_high_vol_breakout(df: pd.DataFrame, lookback: int = 20, vol_mult: float = 1.3) -> pd.Series:
    """close创近lookback根新高 AND volume > vol_ma20×vol_mult"""
    rolling_max = df['close'].rolling(lookback).max().shift(1)
    vm = vol_ma(df, 20)
    return ((df['close'] > rolling_max) & (df['volume'] > vm * vol_mult)).astype(float)


def ma20_rejection_zone(df: pd.DataFrame, tol: float = 0.01) -> pd.Series:
    """价格在MA20±tol区间，收盘未站上MA20"""
    m20 = ma(df, 20)
    near_ma20 = (df['low'] <= m20 * (1 + tol)) & (df['high'] >= m20 * (1 - tol))
    close_below = df['close'] < m20
    return (near_ma20 & close_below).astype(float)


def range_breakdown(df: pd.DataFrame, lookback: int = 20, vol_mult: float = 1.1) -> pd.Series:
    """close < 整理区最低×0.99 AND volume > vol_ma20×vol_mult"""
    consolidation_low = platform_low(df, lookback)
    vm = vol_ma(df, 20)
    return (
        (df['close'] < consolidation_low * 0.99) & (df['volume'] > vm * vol_mult)
    ).astype(float)


def close_recover_after_break(df: pd.DataFrame) -> pd.Series:
    """B2_E01: 跌破次根收盘站回平台（day after break closes above platform_low）"""
    plow = platform_low(df, 30)
    broke = (df['close'] < plow).shift(1).fillna(False)
    recovered = df['close'] > plow
    return (broke & recovered).astype(float)


def rising_lows_3(df: pd.DataFrame, lookback: int = 20, order: int = 3) -> pd.Series:
    """连续3根低点均高于前一低点"""
    return higher_lows_3(df, lookback, order)


def rebound_rejection(df: pd.DataFrame, order: int = 3, tol: float = 0.01) -> pd.Series:
    """价格反弹至前高±tol区间，出现收阴或上影线≥实体×1.2"""
    prev_high = prev_local_high_price(df, order)
    near_prev_high = (df['high'] >= prev_high * (1 - tol)) & (df['high'] <= prev_high * (1 + tol))
    bearish_bar = (df['close'] < df['open']) | (upper_shadow(df) >= body_size(df) * 1.2)
    return (near_prev_high & bearish_bar).astype(float)


def false_breakdown_confirm(df: pd.DataFrame) -> pd.Series:
    """假跌破确认：收回支撑后首根实体完整站回支撑上方"""
    support = support_level(df, 20)
    prev_broke = (df['low'].shift(1) < support.shift(1)).fillna(False)
    body_above = (df[['open', 'close']].min(axis=1) > support)
    return (prev_broke & body_above).astype(float)


def platform_pullback(df: pd.DataFrame, tol: float = 0.01) -> pd.Series:
    """价格在platform_low±tol区间且当根收盘回到平台内"""
    plow  = platform_low(df, 30)
    phigh = platform_high(df, 30)
    near_low = (df['low'] <= plow * (1 + tol)) & (df['low'] >= plow * (1 - tol))
    close_inside = (df['close'] >= plow) & (df['close'] <= phigh)
    return (near_low & close_inside).astype(float)


def ma120_break_confirm(df: pd.DataFrame) -> pd.Series:
    """close < MA120×0.98 并维持≥2根"""
    m120 = ma(df, 120)
    below = (df['close'] < m120 * 0.98).astype(float)
    return below.rolling(2).min()


def ma120_touch_recover(df: pd.DataFrame, tol: float = 0.01) -> pd.Series:
    """价格进入MA120±tol%后1~2根内收回MA120上方"""
    m120 = ma(df, 120)
    near_ma120 = (df['low'] <= m120 * (1 + tol)) & (df['low'] >= m120 * (1 - tol))
    # 当根或次根收回
    recover_same = df['close'] > m120
    recover_next = (df['close'].shift(-1) > m120.shift(-1)).shift(1).fillna(False)
    return (near_ma120 & (recover_same | recover_next)).astype(float)


# ──────────────────────────────────────────────────────────────────────────────
# 3.5  动能类
# ──────────────────────────────────────────────────────────────────────────────

def rsi(df: pd.DataFrame, n: int = 14) -> pd.Series:
    delta = df['close'].diff()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_gain = gain.ewm(com=n - 1, adjust=False).mean()
    avg_loss = loss.ewm(com=n - 1, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def rsi14_local_high(df: pd.DataFrame, order: int = 3) -> pd.Series:
    """局部高点处的RSI值，其他位置填充前值"""
    lh   = local_highs(df, order)
    r14  = rsi(df, 14)
    result = pd.Series(np.nan, index=df.index)
    last_val = 50.0
    for i in range(len(df)):
        if lh.iloc[i] > 0:
            last_val = r14.iloc[i]
        result.iloc[i] = last_val
    return result


def macd_bearish(df: pd.DataFrame) -> pd.Series:
    ema12 = df['close'].ewm(span=12, adjust=False).mean()
    ema26 = df['close'].ewm(span=26, adjust=False).mean()
    dif   = ema12 - ema26
    dea   = dif.ewm(span=9, adjust=False).mean()
    hist  = dif - dea
    return ((hist < 0) & (dif < dea)).astype(float)


# ──────────────────────────────────────────────────────────────────────────────
# 3.6  布尔复合指标
# ──────────────────────────────────────────────────────────────────────────────

def consecutive_lower_highs_3(df: pd.DataFrame, lookback: int = 15, order: int = 3) -> pd.Series:
    return three_lower_highs(df, lookback, order)


def platform_break(df: pd.DataFrame, lookback: int = 30) -> pd.Series:
    """close < platform_low×0.98 且未当根收回"""
    plow = platform_low(df, lookback)
    broke  = df['low'] < plow * 0.98
    closed_below = df['close'] < plow
    return (broke & closed_below).astype(float)


def quick_recovery(df: pd.DataFrame, lookback: int = 3) -> pd.Series:
    """跌破后≤lookback根收盘重新站回支撑之上"""
    plow = platform_low(df, 30)
    broke = (df['low'] < plow * 0.98).shift(1).fillna(False)
    result = pd.Series(0.0, index=df.index)
    for i in range(1, len(df)):
        if broke.iloc[i]:
            # check next lookback bars
            for j in range(i, min(i + lookback, len(df))):
                if df['close'].iloc[j] > plow.iloc[j]:
                    result.iloc[i] = 1.0
                    break
    return result


def wick_only_break(df: pd.DataFrame) -> pd.Series:
    """low < platform_low×0.99 但 close > platform_low"""
    plow = platform_low(df, 30)
    return ((df['low'] < plow * 0.99) & (df['close'] > plow)).astype(float)


def dead_cat_bounce(df: pd.DataFrame) -> pd.Series:
    """跌破后反抽至platform_low±0.5%，收盘未站回"""
    plow = platform_low(df, 30)
    broke   = (df['close'].shift(1) < plow.shift(1)).fillna(False)
    bounced = (df['high'] >= plow * 0.995) & (df['high'] <= plow * 1.005)
    not_recovered = df['close'] < plow
    return (broke & bounced & not_recovered).astype(float)


def ma_rejection(df: pd.DataFrame, tol: float = 0.01) -> pd.Series:
    """价格触及MA20或MA60±1%后收盘未站上"""
    m20 = ma(df, 20)
    m60 = ma(df, 60)
    touched_m20 = (df['low'] <= m20 * (1 + tol)) & (df['high'] >= m20 * (1 - tol))
    touched_m60 = (df['low'] <= m60 * (1 + tol)) & (df['high'] >= m60 * (1 - tol))
    close_below_m20 = df['close'] < m20
    close_below_m60 = df['close'] < m60
    return (
        (touched_m20 & close_below_m20) | (touched_m60 & close_below_m60)
    ).astype(float)


def no_directional_swings(df: pd.DataFrame, lookback: int = 20, order: int = 3) -> pd.Series:
    """近lookback根内无连续3个递升低点也无3个递降高点"""
    no_bull = 1 - higher_lows_3(df, lookback, order)
    no_bear = 1 - three_lower_highs(df, lookback, order)
    return (no_bull * no_bear)


def high_atr_no_direction(df: pd.DataFrame) -> pd.Series:
    """ATR14>atr_ma30×1.4 AND abs(近30根价格净变化)<5%"""
    a14   = atr(df, 14)
    ama30 = atr_ma(df, 14, 30)
    high_vol = a14 > ama30 * 1.4
    start_price = df['close'].shift(29)
    net_change = ((df['close'] - start_price) / start_price.abs().replace(0, np.nan)).abs()
    no_direction = net_change < 0.05
    return (high_vol & no_direction).astype(float)


def outperform_btc_down(df: pd.DataFrame, btc_df: Optional[pd.DataFrame] = None) -> pd.Series:
    """当根跌幅 < BTC同根跌幅（均为负时跌得少）"""
    if btc_df is None or btc_df.empty:
        logger.debug('btc_df not available, outperform_btc_down returns False')
        return pd.Series(0.0, index=df.index)
    self_ret = (df['close'] - df['open']) / df['open'].replace(0, np.nan)
    # align btc returns to df index
    btc_ret  = (btc_df['close'] - btc_df['open']) / btc_df['open'].replace(0, np.nan)
    btc_ret  = btc_ret.reindex(df.index)
    both_neg = (self_ret < 0) & (btc_ret < 0)
    outperform = self_ret > btc_ret
    return (both_neg & outperform).astype(float)


def platform_breakout(df: pd.DataFrame, lookback: int = 30, vol_mult: float = 1.2) -> pd.Series:
    """close > platform_high×1.005 AND volume > vol_ma20×vol_mult"""
    phigh = platform_high(df, lookback)
    vm = vol_ma(df, 20)
    return (
        (df['close'] > phigh * 1.005) & (df['volume'] > vm * vol_mult)
    ).astype(float)


# ──────────────────────────────────────────────────────────────────────────────
# IndicatorLibrary
# ──────────────────────────────────────────────────────────────────────────────

class IndicatorLibrary:
    """
    缓存规范：key = md5(last_ts|nrows|name)[:16]
    scan_latest 内部每次创建局部实例，不跨标的共享。
    """

    def __init__(self, btc_df: Optional[pd.DataFrame] = None):
        self.btc_df  = btc_df
        self._cache: dict[str, pd.Series] = {}

    @staticmethod
    def _make_cache_key(df: pd.DataFrame, name: str) -> str:
        last_ts = str(df.index[-1])
        raw = f'{last_ts}|{len(df)}|{name}'
        return hashlib.md5(raw.encode()).hexdigest()[:16]

    def compute(self, df: pd.DataFrame, name: str) -> pd.Series:
        key = self._make_cache_key(df, name)
        if key not in self._cache:
            try:
                self._cache[key] = self._dispatch(df, name)
            except Exception as e:
                raise IndicatorComputeError(name, str(e)) from e
        result = self._cache[key]
        if result.isna().all():
            raise IndicatorComputeError(name, 'returned all-NaN series')
        return result

    def _dispatch(self, df: pd.DataFrame, name: str) -> pd.Series:
        # ── 参数化指标（regex）──────────────────────────────────────────────
        if m := re.match(r'^ma(\d+)$', name):
            return ma(df, int(m.group(1)))
        if m := re.match(r'^ma(\d+)_slope$', name):
            return ma_slope(df, int(m.group(1)))
        if m := re.match(r'^atr(\d+)$', name):
            return atr(df, int(m.group(1)))
        if m := re.match(r'^atr_ma(\d+)$', name):
            return atr_ma(df, 14, int(m.group(1)))
        if m := re.match(r'^vol_ma(\d+)$', name):
            return vol_ma(df, int(m.group(1)))
        if m := re.match(r'^rsi(\d+)$', name):
            return rsi(df, int(m.group(1)))
        if m := re.match(r'^close_above_ma(\d+)$', name):
            return close_above_ma(df, int(m.group(1)))
        if m := re.match(r'^close_below_ma(\d+)$', name):
            return close_below_ma(df, int(m.group(1)))

        # ── 原始价格列 ───────────────────────────────────────────────────────
        if name == 'close':
            return df['close']
        if name == 'low':
            return df['low']
        if name == 'high':
            return df['high']
        if name == 'open':
            return df['open']
        if name == 'volume':
            return df['volume']

        # ── 命名指标注册表 ───────────────────────────────────────────────────
        fn_map: dict[str, any] = {
            # 均线类
            'ma20_ma60_spread_pct':   lambda: ma20_ma60_spread_pct(df),
            'bear_ma_align':          lambda: bear_ma_align(df),
            'bull_ma_align':          lambda: bull_ma_align(df),
            'ma_order_changes':       lambda: ma_order_changes(df),
            # 波动率类
            'bb_width':               lambda: bb_width(df),
            'bar_range_pct':          lambda: bar_range_pct(df),
            'avg_bar_range':          lambda: avg_bar_range(df),
            'lower_shadow':           lambda: lower_shadow(df),
            'upper_shadow':           lambda: upper_shadow(df),
            'body_size':              lambda: body_size(df),
            'long_wick_count':        lambda: long_wick_count(df),
            # 成交量类
            'vol_ratio':              lambda: vol_ratio(df),
            'bear_vol_spike':         lambda: bear_vol_spike(df),
            'bull_vol_spike':         lambda: bull_vol_spike(df),
            'bull_vol_large':         lambda: bull_vol_large(df),
            'ma60_cross_count':       lambda: ma60_cross_count(df),
            'vol_ma_prev':            lambda: vol_ma_prev(df),
            # 价格结构类
            'local_highs':            lambda: local_highs(df),
            'local_lows':             lambda: local_lows(df),
            'three_lower_highs':      lambda: three_lower_highs(df),
            'higher_lows_3':          lambda: higher_lows_3(df),
            'platform_low':           lambda: platform_low(df),
            'platform_high':          lambda: platform_high(df),
            'max_drawdown_40':        lambda: max_drawdown_40(df),
            'support_break_pct':      lambda: support_break_pct(df),
            'support_touch_count':    lambda: support_touch_count(df),
            'platform_start_price':   lambda: platform_start_price(df),
            'max_advance_45':         lambda: max_advance_45(df),
            'platform_range_ratio':   lambda: platform_range_ratio(df),
            'max_decline_40':         lambda: max_decline_40(df),
            'close_below_platform':   lambda: close_below_platform_series(df),
            'range_bar_count':        lambda: range_bar_series(df),
            'rebound_vol_ratio':      lambda: rebound_vol_ratio(df),
            'prev_local_high':        lambda: prev_local_high_price(df),
            'rebound_high':           lambda: rebound_high_price(df),
            'rebound_slope_ratio':    lambda: rebound_slope_ratio(df),
            'recovered_above_support':lambda: recovered_above_support(df),
            'secondary_breakdown':    lambda: secondary_breakdown(df),
            'triple_ma_suppress':     lambda: triple_ma_suppress(df),
            'close_above_ma60_sustained': lambda: close_above_ma60_sustained(df),
            'recent_low_above_prior': lambda: recent_low_above_prior(df),
            'new_high_vol_breakout':  lambda: new_high_vol_breakout(df),
            'ma20_rejection_zone':    lambda: ma20_rejection_zone(df),
            'range_breakdown':        lambda: range_breakdown(df),
            'close_recover_after_break': lambda: close_recover_after_break(df),
            'rising_lows_3':          lambda: rising_lows_3(df),
            'rebound_rejection':      lambda: rebound_rejection(df),
            'false_breakdown_confirm':lambda: false_breakdown_confirm(df),
            'platform_pullback':      lambda: platform_pullback(df),
            'ma120_break_confirm':    lambda: ma120_break_confirm(df),
            # 动能类
            'rsi14_local_high':       lambda: rsi14_local_high(df),
            'macd_bearish':           lambda: macd_bearish(df),
            # 布尔复合指标
            'consecutive_lower_highs_3': lambda: consecutive_lower_highs_3(df),
            'platform_break':         lambda: platform_break(df),
            'quick_recovery':         lambda: quick_recovery(df),
            'wick_only_break':        lambda: wick_only_break(df),
            'dead_cat_bounce':        lambda: dead_cat_bounce(df),
            'ma_rejection':           lambda: ma_rejection(df),
            'no_directional_swings':  lambda: no_directional_swings(df),
            'high_atr_no_direction':  lambda: high_atr_no_direction(df),
            'outperform_btc_down':    lambda: outperform_btc_down(df, self.btc_df),
            'platform_breakout':      lambda: platform_breakout(df),
            'ma120_touch_recover':    lambda: ma120_touch_recover(df),
        }

        if name in fn_map:
            return fn_map[name]()

        raise IndicatorComputeError(name, f'Unknown indicator: {name}')
