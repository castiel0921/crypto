from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Literal, Optional


OperatorType = Literal[
    ">", "<", ">=", "<=", "==", "!=", "between",
    "ratio_gt", "ratio_lt", "pct_above",
    "cross_above", "cross_below",
    "count_gte", "slope_positive", "slope_negative",
    "all_below", "all_above", "bool_true",
]


@dataclass
class PatternField:
    field_id:       str
    pattern_id:     str
    field_name:     str
    field_type:     Literal["confirm", "exclude", "trigger", "meta"]
    is_required:    bool
    indicator:      str
    operator:       OperatorType
    param_a:        float = 0.0
    param_b:        Optional[float] = None
    lookback:       int   = 1
    weight:         float = 1.0
    penalty:        float = 0.0
    ref_indicator:  Optional[str]  = None
    ref_multiplier: float = 1.0
    description:    str   = ''


@dataclass
class PatternDefinition:
    pattern_id:    str
    pattern_name:  str
    category:      Literal["A", "B", "C"]
    direction:     Literal["long", "short", "neutral"]
    timeframes:    list[str]
    min_bars:      int
    fields:        list[PatternField]
    score_pass:    float = 70.0
    score_high:    float = 85.0
    regime_filter: list[str] = field(default_factory=lambda: ['bull_trend', 'ranging'])
    filter_logic:  Literal["any", "all"] = "any"
    filter_min:    int  = 2
    version:       str  = '1.0'


@dataclass
class PatternScanResult:
    symbol:              str
    timeframe:           str
    bar_time:            datetime
    pattern_id:          str
    pattern_name:        str
    direction:           str
    regime:              str
    regime_score:        float
    total_score:         float
    confirm_score:       float
    exclude_penalty:     float
    field_results:       dict[str, bool]
    raw_values:          dict[str, float]
    trigger_met:         bool
    trigger_type:        Optional[str]  = None
    invalidated:         bool           = False
    invalidation_reason: Optional[str]  = None
    is_filter_hit:       bool           = False
    scan_batch_id:       Optional[str]  = None
    db_id:               Optional[int]  = None
    llm_confidence:      Optional[str]  = None
    llm_enter_pool:      Optional[bool] = None
    llm_risk:            Optional[str]  = None
    llm_reasoning:       Optional[str]  = None
    rule_version:        Optional[str]  = None


@dataclass
class PipelineResult:
    symbol:           str
    timeframe:        str
    batch_id:         str
    regime:           str
    regime_score:     float
    scan_results:     list[PatternScanResult]
    candidate_count:  int
    llm_task_id:      Optional[str]
    scan_duration_ms: float
    error:            Optional[str]


@dataclass
class SymbolCacheHealth:
    symbol:       str
    bars_count:   int
    latest_time:  Optional[datetime]
    oldest_time:  Optional[datetime]
    gap_count:    int
    is_fresh:     bool
    is_scannable: bool


@dataclass
class JobSummary:
    batch_id:               str
    symbols_total:          int
    symbols_fetched:        int
    symbols_fetch_failed:   int
    symbols_scannable:      int
    symbols_skipped_bars:   int
    symbols_high_vol_skip:  int
    symbols_filter_hit:     int
    patterns_found:         int
    patterns_high_quality:  int
    patterns_trigger_met:   int
    llm_reviewed_count:     int
    llm_success_count:      int
    llm_timeout_count:      int
    llm_circuit_broken:     bool
    duration_fetch_sec:     float
    duration_scan_sec:      float
    duration_llm_sec:       float
    duration_total_sec:     float


@dataclass
class DataFetchLog:
    batch_id:        str
    interval:        str
    symbols_total:   int
    symbols_success: int
    symbols_failed:  int
    failed_symbols:  list[str]
    duration_sec:    float
    status:          str
    triggered_at:    Optional[datetime] = None


@dataclass
class PatternBacktestStats:
    pattern_id:            str
    regime:                str
    timeframe:             str
    forward_bars:          int
    trigger_only:          bool
    sample_size:           int
    win_rate:              float
    avg_return:            float
    avg_holding_bars:      float
    max_drawdown:          float
    sharpe_like:           float
    llm_high_conf_win_rate: Optional[float] = None
    stat_period_start:     Optional[datetime] = None
    stat_period_end:       Optional[datetime] = None


@dataclass
class PipelineRunLog:
    job_id:          str
    batch_id:        str
    interval:        str
    triggered_at:    datetime
    status:          str
    stage:           Optional[str]   = None
    finished_at:     Optional[datetime] = None
    symbols_total:   Optional[int]   = None
    symbols_fetched: Optional[int]   = None
    patterns_found:  Optional[int]   = None
    llm_reviewed:    Optional[int]   = None
    llm_success:     Optional[int]   = None
    llm_timeout:     Optional[int]   = None
    error_stage:     Optional[str]   = None
    error_message:   Optional[str]   = None
    failed_symbols:  Optional[list]  = None
    duration_sec:    Optional[float] = None


class Regime(str, Enum):
    BULL_TREND = 'bull_trend'
    RANGING    = 'ranging'
    BEAR_TREND = 'bear_trend'
    HIGH_VOL   = 'high_vol'


@dataclass
class RegimeResult:
    regime:       Regime
    score:        float
    trend_score:  float
    vol_score:    float
    volume_score: float
    btc_score:    float
    meta:         dict
