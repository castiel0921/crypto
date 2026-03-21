"""
LLMClient — LLM调用基础层（支持 Anthropic 和 OpenAI 兼容接口）

自动根据 base_url 判断协议：
  - api.anthropic.com  → Anthropic Messages API
  - 其他（DeepSeek等） → OpenAI Chat Completions API

内置熔断器：连续失败5次开启，300s 冷却后自动恢复。
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
CIRCUIT_BREAKER_COOLDOWN  = 300


class CircuitBreakerOpen(Exception):
    """熔断器处于开启状态"""


class LLMClient:
    """
    通用 LLM 客户端，自动适配 Anthropic / OpenAI 兼容接口。

    DeepSeek 示例：
        LLMClient(
            api_key  = 'sk-xxx',
            base_url = 'https://api.deepseek.com',
            model    = 'deepseek-chat',
        )

    Anthropic 示例：
        LLMClient(
            api_key  = 'sk-ant-xxx',
            base_url = 'https://api.anthropic.com',
            model    = 'claude-haiku-4-5-20251001',
        )
    """

    def __init__(
        self,
        api_key:     str,
        base_url:    str = 'https://api.deepseek.com',
        model:       str = 'deepseek-chat',
        timeout_sec: float = 30.0,
        max_tokens:  int   = 1024,
    ):
        self._api_key    = api_key
        self._base_url   = base_url.rstrip('/')
        self._model      = model
        self._timeout    = timeout_sec
        self._max_tokens = max_tokens
        self._is_anthropic = 'anthropic.com' in self._base_url

        self._failure_count: int            = 0
        self._opened_at: Optional[float]    = None

    # ── 熔断器 ────────────────────────────────────────────────────────────────

    def is_open(self) -> bool:
        if self._failure_count < CIRCUIT_BREAKER_THRESHOLD:
            return False
        if self._opened_at is None:
            return False
        if time.monotonic() - self._opened_at >= CIRCUIT_BREAKER_COOLDOWN:
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
            logger.warning('Circuit breaker OPENED after %d failures', self._failure_count)

    def _record_success(self) -> None:
        self._failure_count = 0
        self._opened_at     = None

    # ── 请求 ──────────────────────────────────────────────────────────────────

    async def chat(
        self,
        system_prompt: str,
        user_message:  str,
        temperature:   float = 0.2,
    ) -> str:
        """发送对话请求，返回 assistant 回复文本。"""
        if self.is_open():
            raise CircuitBreakerOpen('LLM circuit breaker is open')

        try:
            if self._is_anthropic:
                text = await self._call_anthropic(system_prompt, user_message, temperature)
            else:
                text = await self._call_openai_compat(system_prompt, user_message, temperature)
            self._record_success()
            return text

        except (CircuitBreakerOpen, LLMCallError, LLMParseError):
            raise
        except httpx.TimeoutException as e:
            self._record_failure()
            raise LLMCallError(f'LLM timeout: {e}') from e
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
        """发送请求并将响应解析为 JSON。"""
        text = await self.chat(system_prompt, user_message, temperature)
        return _extract_json(text)

    # ── Anthropic Messages API ────────────────────────────────────────────────

    async def _call_anthropic(
        self, system_prompt: str, user_message: str, temperature: float
    ) -> str:
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
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f'{self._base_url}/v1/messages',
                headers=headers,
                json=payload,
            )
        return self._parse_anthropic(resp)

    def _parse_anthropic(self, resp: httpx.Response) -> str:
        if resp.status_code == 429:
            self._record_failure()
            raise LLMCallError(f'Rate limited (429): {resp.text[:200]}')
        if resp.status_code != 200:
            self._record_failure()
            raise LLMCallError(f'HTTP {resp.status_code}: {resp.text[:400]}')
        data    = resp.json()
        content = data.get('content', [])
        if not content:
            self._record_failure()
            raise LLMParseError('Empty content in Anthropic response')
        return content[0].get('text', '')

    # ── OpenAI 兼容接口（DeepSeek / OpenAI / 其他）────────────────────────────

    async def _call_openai_compat(
        self, system_prompt: str, user_message: str, temperature: float
    ) -> str:
        headers = {
            'Authorization': f'Bearer {self._api_key}',
            'Content-Type':  'application/json',
        }
        payload = {
            'model':      self._model,
            'max_tokens': self._max_tokens,
            'temperature': temperature,
            'messages': [
                {'role': 'system', 'content': system_prompt},
                {'role': 'user',   'content': user_message},
            ],
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            resp = await client.post(
                f'{self._base_url}/v1/chat/completions',
                headers=headers,
                json=payload,
            )
        return self._parse_openai_compat(resp)

    def _parse_openai_compat(self, resp: httpx.Response) -> str:
        if resp.status_code == 429:
            self._record_failure()
            raise LLMCallError(f'Rate limited (429): {resp.text[:200]}')
        if resp.status_code != 200:
            self._record_failure()
            raise LLMCallError(f'HTTP {resp.status_code}: {resp.text[:400]}')
        data    = resp.json()
        choices = data.get('choices', [])
        if not choices:
            self._record_failure()
            raise LLMParseError('Empty choices in LLM response')
        return choices[0].get('message', {}).get('content', '')


def _extract_json(text: str) -> dict[str, Any]:
    """从 LLM 响应中提取 JSON（支持 ```json ... ``` 包裹）"""
    text = text.strip()
    if '```' in text:
        start = text.find('```')
        end   = text.rfind('```')
        if start != end:
            block = text[start + 3: end].strip()
            if block.startswith('json'):
                block = block[4:].strip()
            text = block
    brace_start = text.find('{')
    brace_end   = text.rfind('}')
    if brace_start != -1 and brace_end > brace_start:
        text = text[brace_start: brace_end + 1]
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        raise LLMParseError(f'JSON parse failed: {e} | text={text[:300]}') from e
