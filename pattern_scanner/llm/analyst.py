"""
LLMAnalyst — 综合形态分析（Section 07，插入点B）

职责：
- 汇聚同一个 symbol/timeframe 下的多个候选
- 调用 LLMClient 进行深度分析
- 返回 LLMAnalystOutput
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from ..exceptions import LLMParseError
from ..models import PatternScanResult, RegimeResult
from .base import LLMClient
from .schemas import AnalystCandidate, LLMAnalystInput, LLMAnalystOutput

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / 'prompts' / 'analyst_v1.txt'


def build_analyst_input(
    symbol: str,
    timeframe: str,
    candidates: list[PatternScanResult],
    regime_result: Optional[RegimeResult] = None,
    btc_context: Optional[dict] = None,
) -> LLMAnalystInput:
    """将扫描结果列表构建为 LLMAnalystInput"""
    analyst_candidates = [
        AnalystCandidate(
            symbol       = r.symbol,
            timeframe    = r.timeframe,
            pattern_id   = r.pattern_id,
            pattern_name = r.pattern_name,
            direction    = r.direction,
            regime       = r.regime,
            total_score  = r.total_score,
            trigger_met  = r.trigger_met,
            llm_confidence = r.llm_confidence,
            bar_time     = r.bar_time.isoformat() if r.bar_time else '',
            db_id        = r.db_id,
            raw_summary  = {
                k: round(v, 4) for k, v in (r.raw_values or {}).items()
            },
        )
        for r in candidates
    ]

    regime_summary = {}
    if regime_result:
        regime_summary = {
            'regime':       regime_result.regime.value,
            'score':        regime_result.score,
            'trend_score':  regime_result.trend_score,
            'vol_score':    regime_result.vol_score,
            'volume_score': regime_result.volume_score,
            **regime_result.meta,
        }

    return LLMAnalystInput(
        symbol         = symbol,
        timeframe      = timeframe,
        candidates     = analyst_candidates,
        regime_summary = regime_summary,
        btc_context    = btc_context,
    )


class LLMAnalyst:
    """
    深度综合分析器。
    每次调用 analyze() 针对一个 symbol/timeframe 下的所有候选形态。
    """

    def __init__(self, client: LLMClient):
        self._client       = client
        self._system_prompt = _PROMPT_PATH.read_text(encoding='utf-8')

    async def analyze(self, inp: LLMAnalystInput) -> LLMAnalystOutput:
        """
        对多个候选形态进行综合分析。

        Raises:
            CircuitBreakerOpen: 熔断器开启
            LLMCallError: HTTP错误
            LLMParseError: 响应解析失败
        """
        user_msg = _build_analyst_message(inp)
        data     = await self._client.chat_json(self._system_prompt, user_msg)
        return _parse_analyst_output(data, inp)


def _build_analyst_message(inp: LLMAnalystInput) -> str:
    candidates_data = [
        {
            'pattern_id':   c.pattern_id,
            'pattern_name': c.pattern_name,
            'direction':    c.direction,
            'total_score':  c.total_score,
            'trigger_met':  c.trigger_met,
            'llm_confidence': c.llm_confidence,
            'bar_time':     c.bar_time,
            'raw_summary':  c.raw_summary,
        }
        for c in inp.candidates
    ]

    payload: dict = {
        'symbol':         inp.symbol,
        'timeframe':      inp.timeframe,
        'candidates':     candidates_data,
        'regime_summary': inp.regime_summary,
    }
    if inp.btc_context:
        payload['btc_context'] = inp.btc_context

    return json.dumps(payload, ensure_ascii=False, indent=2)


def _parse_analyst_output(data: dict, inp: LLMAnalystInput) -> LLMAnalystOutput:
    try:
        direction_bias = data.get('direction_bias', 'neutral')
        if direction_bias not in ('long', 'short', 'neutral'):
            direction_bias = 'neutral'

        confidence = data.get('confidence', 'low')
        if confidence not in ('high', 'medium', 'low'):
            confidence = 'low'

        risk_reward = data.get('risk_reward')
        if risk_reward is not None:
            try:
                risk_reward = float(risk_reward)
            except (TypeError, ValueError):
                risk_reward = None

        # 如果没指定 primary_pattern，取第一个候选的 pattern_id
        primary = str(data.get('primary_pattern', ''))
        if not primary and inp.candidates:
            primary = inp.candidates[0].pattern_id

        return LLMAnalystOutput(
            symbol            = inp.symbol,
            timeframe         = inp.timeframe,
            primary_pattern   = primary,
            direction_bias    = direction_bias,
            entry_suggestion  = str(data.get('entry_suggestion', ''))[:500],
            stop_suggestion   = str(data.get('stop_suggestion', ''))[:300],
            target_suggestion = str(data.get('target_suggestion', ''))[:300],
            risk_reward       = risk_reward,
            reasoning         = str(data.get('reasoning', ''))[:1000],
            confidence        = confidence,
            tags              = list(data.get('tags', [])),
        )
    except Exception as e:
        raise LLMParseError(f'AnalystOutput parse failed: {e} | data={data}') from e
