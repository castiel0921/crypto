"""
LLMNarrator — 自然语言报告生成（Section 07，插入点C）

职责：
- 将 LLMAnalystOutput 转换为人类可读的 Markdown 报告
- 返回 LLMNarratorOutput
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from ..exceptions import LLMParseError
from .base import LLMClient
from .schemas import LLMAnalystOutput, LLMNarratorInput, LLMNarratorOutput

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent / 'prompts' / 'narrator_v1.txt'


class LLMNarrator:
    """
    自然语言叙述生成器。
    将结构化分析报告转换为 Markdown 格式的可读报告。
    """

    def __init__(self, client: LLMClient):
        self._client       = client
        self._system_prompt = _PROMPT_PATH.read_text(encoding='utf-8')

    async def narrate(self, inp: LLMNarratorInput) -> LLMNarratorOutput:
        """
        生成自然语言报告。

        Raises:
            CircuitBreakerOpen: 熔断器开启
            LLMCallError: HTTP错误
            LLMParseError: 响应解析失败
        """
        user_msg = _build_narrator_message(inp)
        data     = await self._client.chat_json(self._system_prompt, user_msg)
        return _parse_narrator_output(data, inp)


def _build_narrator_message(inp: LLMNarratorInput) -> str:
    r = inp.analyst_report
    payload = {
        'symbol':           inp.symbol,
        'timeframe':        inp.timeframe,
        'primary_pattern':  r.primary_pattern,
        'direction_bias':   r.direction_bias,
        'entry_suggestion': r.entry_suggestion,
        'stop_suggestion':  r.stop_suggestion,
        'target_suggestion': r.target_suggestion,
        'risk_reward':      r.risk_reward,
        'confidence':       r.confidence,
        'reasoning':        r.reasoning,
        'tags':             r.tags,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2)


def _parse_narrator_output(data: dict, inp: LLMNarratorInput) -> LLMNarratorOutput:
    try:
        narrative = str(data.get('narrative', ''))
        summary   = str(data.get('summary', ''))[:100]
        tags      = list(data.get('tags', []))

        if not narrative:
            raise LLMParseError('Empty narrative in narrator output')

        return LLMNarratorOutput(
            symbol    = inp.symbol,
            timeframe = inp.timeframe,
            narrative = narrative,
            summary   = summary,
            tags      = tags,
        )
    except LLMParseError:
        raise
    except Exception as e:
        raise LLMParseError(f'NarratorOutput parse failed: {e} | data={data}') from e
