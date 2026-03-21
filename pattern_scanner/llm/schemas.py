"""
LLM 输入/输出 Schema 定义（Section 07）
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional


# ── Reviewer（A插入点）────────────────────────────────────────────────────────

@dataclass
class LLMReviewerInput:
    """提交给 LLMReviewer 的单个形态候选数据"""
    symbol:       str
    timeframe:    str
    pattern_id:   str
    pattern_name: str
    direction:    str
    regime:       str
    regime_score: float
    total_score:  float
    trigger_met:  bool
    field_results: dict[str, bool]
    raw_values:   dict[str, float]
    bar_time:     str                # ISO格式
    scan_batch_id: Optional[str] = None


@dataclass
class LLMReviewerOutput:
    """LLMReviewer 的输出（结构化JSON）"""
    confidence:  Literal['high', 'medium', 'low']
    enter_pool:  bool
    risk:        Literal['low', 'medium', 'high']
    reasoning:   str
    # 原始候选引用
    symbol:      str = ''
    pattern_id:  str = ''
    db_id:       Optional[int] = None


# ── Analyst（B插入点）────────────────────────────────────────────────────────

@dataclass
class AnalystCandidate:
    """进入 Analyst 分析的候选（多空均衡的多个形态）"""
    symbol:       str
    timeframe:    str
    pattern_id:   str
    pattern_name: str
    direction:    str
    regime:       str
    total_score:  float
    trigger_met:  bool
    llm_confidence: Optional[str]
    bar_time:     str
    db_id:        Optional[int] = None
    raw_summary:  dict = field(default_factory=dict)


@dataclass
class LLMAnalystInput:
    """提交给 LLMAnalyst 的完整分析请求"""
    symbol:         str
    timeframe:      str
    candidates:     list[AnalystCandidate]
    regime_summary: dict                   # 体制元数据
    btc_context:    Optional[dict] = None  # BTC近期走势摘要
    extra_context:  Optional[str]  = None


@dataclass
class LLMAnalystOutput:
    """LLMAnalyst 输出的综合分析报告"""
    symbol:          str
    timeframe:       str
    primary_pattern: str      # 推荐的 pattern_id
    direction_bias:  Literal['long', 'short', 'neutral']
    entry_suggestion: str
    stop_suggestion:  str
    target_suggestion: str
    risk_reward:     Optional[float]
    reasoning:       str
    confidence:      Literal['high', 'medium', 'low']
    tags:            list[str] = field(default_factory=list)


# ── Narrator（C插入点）───────────────────────────────────────────────────────

@dataclass
class LLMNarratorInput:
    """提交给 LLMNarrator 的报告输入"""
    symbol:         str
    timeframe:      str
    analyst_report: LLMAnalystOutput
    scan_batch_id:  Optional[str] = None


@dataclass
class LLMNarratorOutput:
    """LLMNarrator 输出的自然语言叙述"""
    symbol:     str
    timeframe:  str
    narrative:  str          # 完整叙述文本（Markdown格式）
    summary:    str          # 一句话摘要
    tags:       list[str] = field(default_factory=list)
