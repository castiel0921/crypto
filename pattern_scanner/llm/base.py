"""
LLMClient — LLM调用基础层（Section 07）

设计原则：
1. 单一职责：只负责 HTTP 调用和 JSON 解析，不含业务逻辑
2. 熔断机制：连续失败5次后开启熔断，冷却300s后自动恢复
3. 超时控制：默认30s，可配置
4. 错误封装：统一抛出 LLMCallError / LLMParseError
"""
from __future__ import annotations

import json
import logging
import time
from typing import Any, Optional

import httpx

from ..exceptions import LLMCallError, LLMParseError

logger = logging.getLogger(__name__)

CIRCUIT_BREAKER_THRESHOLD = 5
CIRCUIT_BREAKER_COOLDOWN  = 300  # seconds


class CircuitBreakerOpen(Exception):
    """熔断器处于开启状态"""


class LLMClient:
    """
    通用 LLM HTTP 客户端（OpenAI-compatible API）。
    内置熔断器，防止大面积失败时雪崩。
    """

    def __init__(
        self,
        api_key: str,
        base_url: str = 'https://api.anthropic.com',
        model: str = 'claude-haiku-4-5-20251001',
        timeout_sec: float = 30.0,
        max_tokens: int = 1024,
    ):
        self._api_key    = api_key
        self._base_url   = base_url.rstrip('/')
        self._model      = model
        self._timeout    = timeout_sec
        self._max_tokens = max_tokens

        # 熔断器状态
        self._failure_count: int   = 0
        self._opened_at: Optional[float] = None

    # ──────────────────────────────────────────────────────────────────────────

    def is_open(self) -> bool:
        """熔断器是否处于开启（拒绝请求）状态"""
        if self._failure_count < CIRCUIT_BREAKER_THRESHOLD:
            return False
        if self._opened_at is None:
            return False
        elapsed = time.monotonic() - self._opened_at
        if elapsed >= CIRCUIT_BREAKER_COOLDOWN:
            # 冷却完成，半开状态允许重试
            logger.info('Circuit breaker entering half-open state')
            self._failure_count = 0
            self._opened_at     = None
            return False
        return True

    def reset_circuit(self) -> None:
        self._failure_count = 0
        self._opened_at     = None

    def _record_failure(self) -> None:
        self._failure_count += 1
        if self._failure_count >= CIRCUIT_BREAKER_THRESHOLD and self._opened_at is None:
            self._opened_at = time.monotonic()
            logger.warning(
                'Circuit breaker OPENED after %d consecutive failures',
                self._failure_count,
            )

    def _record_success(self) -> None:
        self._failure_count = 0
        self._opened_at     = None

    # ──────────────────────────────────────────────────────────────────────────

    async def chat(
        self,
        system_prompt: str,
        user_message:  str,
        temperature:   float = 0.2,
    ) -> str:
        """
        发送 chat completion 请求，返回 assistant 回复文本。

        Raises:
            CircuitBreakerOpen: 熔断器开启中
            LLMCallError: HTTP/网络错误
            LLMParseError: 响应解析失败
        """
        if self.is_open():
            raise CircuitBreakerOpen('LLM circuit breaker is open')

        headers = {
            'x-api-key':         self._api_key,
            'anthropic-version': '2023-06-01',
            'content-type':      'application/json',
        }
        payload = {
            'model':      self._model,
            'max_tokens': self._max_tokens,
            'temperature': temperature,
            'system':     system_prompt,
            'messages':   [{'role': 'user', 'content': user_message}],
        }

        try:
            async with httpx.AsyncClient(timeout=self._timeout) as client:
                resp = await client.post(
                    f'{self._base_url}/v1/messages',
                    headers=headers,
                    json=payload,
                )

            if resp.status_code == 429:
                self._record_failure()
                raise LLMCallError(f'Rate limited (429): {resp.text[:200]}')

            if resp.status_code != 200:
                self._record_failure()
                raise LLMCallError(f'HTTP {resp.status_code}: {resp.text[:400]}')

            data = resp.json()
            content = data.get('content', [])
            if not content:
                self._record_failure()
                raise LLMParseError('Empty content in LLM response')

            text = content[0].get('text', '')
            self._record_success()
            return text

        except (CircuitBreakerOpen, LLMCallError, LLMParseError):
            raise
        except httpx.TimeoutException as e:
            self._record_failure()
            raise LLMCallError(f'LLM request timeout: {e}') from e
        except httpx.RequestError as e:
            self._record_failure()
            raise LLMCallError(f'LLM request error: {e}') from e
        except Exception as e:
            self._record_failure()
            raise LLMCallError(f'Unexpected LLM error: {e}') from e

    async def chat_json(
        self,
        system_prompt: str,
        user_message:  str,
        temperature:   float = 0.1,
    ) -> dict[str, Any]:
        """
        发送请求并将响应解析为 JSON。
        自动提取 markdown 代码块中的 JSON。

        Raises:
            LLMParseError: JSON 解析失败
        """
        text = await self.chat(system_prompt, user_message, temperature)
        return _extract_json(text)


def _extract_json(text: str) -> dict[str, Any]:
    """从 LLM 响应文本中提取 JSON（支持 ```json ... ``` 包裹）"""
    text = text.strip()

    # 尝试提取 markdown 代码块
    if '```' in text:
        start = text.find('```')
        end   = text.rfind('```')
        if start != end:
            block = text[start + 3: end].strip()
            if block.startswith('json'):
                block = block[4:].strip()
            text = block

    # 尝试找到第一个 { ... } 结构
    brace_start = text.find('{')
    brace_end   = text.rfind('}')
    if brace_start != -1 and brace_end > brace_start:
        text = text[brace_start: brace_end + 1]

    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise LLMParseError(f'JSON parse failed: {e} | text={text[:300]}') from e
