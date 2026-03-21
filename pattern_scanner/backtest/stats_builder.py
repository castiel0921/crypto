"""
BacktestStatsBuilder — 形态历史胜率统计（Section 09）

从数据库加载历史扫描结果，统计各形态在不同体制/时间框架下的：
- 胜率（win_rate）
- 平均收益（avg_return）
- 最大回撤（max_drawdown）
- 类夏普比（sharpe_like）
- LLM 高置信度命中率
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import numpy as np
import pandas as pd

from ..database.repository import PatternRepository
from ..models import PatternBacktestStats

logger = logging.getLogger(__name__)

# 前向收益计算窗口
DEFAULT_FORWARD_BARS = [4, 12, 24]  # 对应 4h K线的 16h, 48h, 96h


@dataclass
class BacktestConfig:
    forward_bars:   int   = 12      # 持仓窗口（K线数）
    win_threshold:  float = 0.01    # 1% 收益视为获胜
    trigger_only:   bool  = False   # 只统计 trigger_met=True 的记录
    min_sample:     int   = 10      # 最小样本量


class BacktestStatsBuilder:
    """
    形态历史统计构建器。
    依赖：PatternRepository 获取历史扫描结果和K线数据。
    """

    def __init__(self, repository: PatternRepository):
        self._repo = repository

    async def build_all(
        self,
        kline_data: dict[str, pd.DataFrame],
        config: BacktestConfig = BacktestConfig(),
        pattern_ids: Optional[list[str]] = None,
        timeframes:  Optional[list[str]] = None,
    ) -> list[PatternBacktestStats]:
        """
        为所有形态/体制/时间框架组合计算统计数据，写入数据库。

        Args:
            kline_data: {symbol -> DataFrame} 的历史K线数据
            config:     回测配置
            pattern_ids: 限定形态ID列表（None=全部）
            timeframes:  限定时间框架列表（None=全部）

        Returns:
            所有计算出的 PatternBacktestStats 列表
        """
        # 从数据库加载历史扫描记录
        records = await self._repo.get_scan_history(
            pattern_ids  = pattern_ids,
            timeframes   = timeframes,
            trigger_only = config.trigger_only,
        )

        if not records:
            logger.warning('No historical scan records found')
            return []

        df_records = pd.DataFrame(records)

        # 按 pattern_id / regime / timeframe 分组计算
        group_keys = ['pattern_id', 'regime', 'timeframe']
        all_stats: list[PatternBacktestStats] = []

        for keys, group in df_records.groupby(group_keys):
            pattern_id, regime, timeframe = keys

            stats = self._compute_stats(
                group      = group,
                kline_data = kline_data,
                pattern_id = pattern_id,
                regime     = regime,
                timeframe  = timeframe,
                config     = config,
            )
            if stats is not None:
                all_stats.append(stats)

        # 写入数据库（repository 期望列表）
        if all_stats:
            await self._repo.upsert_backtest_stats(all_stats)

        logger.info('BacktestStats built: %d groups', len(all_stats))
        return all_stats

    def _compute_stats(
        self,
        group:      pd.DataFrame,
        kline_data: dict[str, pd.DataFrame],
        pattern_id: str,
        regime:     str,
        timeframe:  str,
        config:     BacktestConfig,
    ) -> Optional[PatternBacktestStats]:
        if len(group) < config.min_sample:
            return None

        returns:       list[float] = []
        holding_bars:  list[float] = []
        llm_high_wins: list[bool]  = []
        llm_high_total: int        = 0

        for _, row in group.iterrows():
            symbol   = row.get('symbol', '')
            bar_time = row.get('bar_time')
            direction = row.get('direction', 'long')

            kdf = kline_data.get(symbol)
            if kdf is None or kdf.empty:
                continue

            ret, bars = _calc_forward_return(
                kdf, bar_time, config.forward_bars, direction
            )
            if ret is None:
                continue

            returns.append(ret)
            holding_bars.append(float(bars))

            llm_conf = row.get('llm_confidence')
            if llm_conf == 'high':
                llm_high_total += 1
                llm_high_wins.append(ret > config.win_threshold)

        if len(returns) < config.min_sample:
            return None

        arr = np.array(returns)
        win_rate    = float((arr > config.win_threshold).mean())
        avg_return  = float(arr.mean())
        max_dd      = float(_max_drawdown(arr))
        sharpe_like = float(arr.mean() / arr.std()) if arr.std() > 0 else 0.0

        llm_win_rate: Optional[float] = None
        if llm_high_total >= 3:
            llm_win_rate = float(sum(llm_high_wins) / llm_high_total)

        return PatternBacktestStats(
            pattern_id            = pattern_id,
            regime                = regime,
            timeframe             = timeframe,
            forward_bars          = config.forward_bars,
            trigger_only          = config.trigger_only,
            sample_size           = len(returns),
            win_rate              = round(win_rate, 4),
            avg_return            = round(avg_return, 6),
            avg_holding_bars      = round(float(np.mean(holding_bars)), 2),
            max_drawdown          = round(max_dd, 6),
            sharpe_like           = round(sharpe_like, 4),
            llm_high_conf_win_rate = round(llm_win_rate, 4) if llm_win_rate else None,
            stat_period_start     = _safe_min_time(group),
            stat_period_end       = _safe_max_time(group),
        )


# ──────────────────────────────────────────────────────────────────────────────

def _calc_forward_return(
    kdf:      pd.DataFrame,
    bar_time,
    n_bars:   int,
    direction: str,
) -> tuple[Optional[float], int]:
    """
    计算在 bar_time 时刻入场，持有 n_bars 后的收益率。
    direction='long' 时做多，'short' 时做空。
    """
    try:
        # 找到 bar_time 对应的索引位置
        if isinstance(bar_time, str):
            bar_time = pd.Timestamp(bar_time)

        idx = kdf.index.get_indexer([bar_time], method='nearest')[0]
        if idx < 0 or idx + n_bars >= len(kdf):
            return None, 0

        entry_price = float(kdf['close'].iloc[idx])
        exit_price  = float(kdf['close'].iloc[idx + n_bars])

        if entry_price == 0:
            return None, 0

        ret = (exit_price - entry_price) / entry_price
        if direction == 'short':
            ret = -ret

        return ret, n_bars
    except Exception:
        return None, 0


def _max_drawdown(returns: np.ndarray) -> float:
    """计算累积收益序列的最大回撤"""
    cumulative = np.cumprod(1 + returns)
    rolling_max = np.maximum.accumulate(cumulative)
    drawdowns = (cumulative - rolling_max) / rolling_max
    return float(drawdowns.min()) if len(drawdowns) > 0 else 0.0


def _safe_min_time(group: pd.DataFrame) -> Optional[datetime]:
    col = 'bar_time'
    if col not in group.columns:
        return None
    try:
        return pd.Timestamp(group[col].min()).to_pydatetime()
    except Exception:
        return None


def _safe_max_time(group: pd.DataFrame) -> Optional[datetime]:
    col = 'bar_time'
    if col not in group.columns:
        return None
    try:
        return pd.Timestamp(group[col].max()).to_pydatetime()
    except Exception:
        return None
