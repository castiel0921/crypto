"""
LLMReviewer — 形态候选快速审核（Section 07，插入点A）

职责：
- 将 PatternScanResult 转换为 LLMReviewerInput
- 调用 LLMClient 获取审核结果
- 解析并返回 LLMReviewerOutput
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from ..exceptions import LLMParseError
from ..models import PatternScanResult
from .base import LLMClient
from .schemas import LLMReviewerInput, LLMReviewerOutput

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / 'prompts' / 'reviewer_v1.txt'


def scan_result_to_reviewer_input(result: PatternScanResult) -> LLMReviewerInput:
    """将 PatternScanResult 转换为 LLMReviewerInput"""
    return LLMReviewerInput(
        symbol        = result.symbol,
        timeframe     = result.timeframe,
        pattern_id    = result.pattern_id,
        pattern_name  = result.pattern_name,
        direction     = result.direction,
        regime        = result.regime,
        regime_score  = result.regime_score,
        total_score   = result.total_score,
        trigger_met   = result.trigger_met,
        field_results  = result.field_results,
        raw_values    = result.raw_values,
        bar_time      = result.bar_time.isoformat() if result.bar_time else '',
        scan_batch_id = result.scan_batch_id,
    )


class LLMReviewer:
    """
    形态候选的快速LLM审核器。
    每次调用 review() 对应一个形态候选。
    """

    def __init__(self, client: LLMClient):
        self._client      = client
        self._system_prompt = _PROMPT_PATH.read_text(encoding='utf-8')

    async def review(
        self,
        inp: LLMReviewerInput,
        db_id: Optional[int] = None,
    ) -> LLMReviewerOutput:
        """
        审核单个形态候选。

        Raises:
            CircuitBreakerOpen: 熔断器开启
            LLMCallError: HTTP错误
            LLMParseError: 响应解析失败
        """
        user_msg = _build_reviewer_message(inp)
        data     = await self._client.chat_json(self._system_prompt, user_msg)

        return _parse_reviewer_output(data, inp, db_id)


def _build_reviewer_message(inp: LLMReviewerInput) -> str:
    """构建发送给 Reviewer 的用户消息"""
    # 只发送关键字段，控制 token 消耗
    key_fields = {k: v for k, v in inp.field_results.items() if v}  # 只保留命中的字段
    key_raws   = {k: round(v, 4) for k, v in inp.raw_values.items()
                  if k in key_fields or abs(v) > 0.01}

    return json.dumps({
        'symbol':       inp.symbol,
        'timeframe':    inp.timeframe,
        'pattern_id':   inp.pattern_id,
        'pattern_name': inp.pattern_name,
        'direction':    inp.direction,
        'regime':       inp.regime,
        'regime_score': inp.regime_score,
        'total_score':  inp.total_score,
        'trigger_met':  inp.trigger_met,
        'hit_fields':   key_fields,
        'raw_values':   key_raws,
        'bar_time':     inp.bar_time,
    }, ensure_ascii=False, indent=2)


def _parse_reviewer_output(
    data: dict,
    inp: LLMReviewerInput,
    db_id: Optional[int],
) -> LLMReviewerOutput:
    try:
        confidence = data.get('confidence', 'low')
        if confidence not in ('high', 'medium', 'low'):
            confidence = 'low'

        enter_pool = bool(data.get('enter_pool', False))

        risk = data.get('risk', 'medium')
        if risk not in ('low', 'medium', 'high'):
            risk = 'medium'

        reasoning = str(data.get('reasoning', ''))[:500]

        return LLMReviewerOutput(
            confidence  = confidence,
            enter_pool  = enter_pool,
            risk        = risk,
            reasoning   = reasoning,
            symbol      = inp.symbol,
            pattern_id  = inp.pattern_id,
            db_id       = db_id,
        )
    except Exception as e:
        raise LLMParseError(f'ReviewerOutput parse failed: {e} | data={data}') from e
