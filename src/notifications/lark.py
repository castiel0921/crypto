from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import urllib.request
from typing import Any

import math

from arbitrage import Opportunity


def _fmt(value: float, sig: int = 4) -> str:
    """Format a float to *sig* significant figures without scientific notation."""
    if value == 0:
        return "0"
    abs_val = abs(value)
    if abs_val >= 1:
        int_digits = int(math.log10(abs_val)) + 1
        decimals = max(0, sig - int_digits)
    else:
        leading_zeros = -int(math.floor(math.log10(abs_val))) - 1
        decimals = leading_zeros + sig
    return f"{value:.{decimals}f}"


class LarkNotifier:
    def __init__(
        self,
        webhook_url: str,
        *,
        sign_secret: str = "",
        dashboard_url: str = "",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.webhook_url = webhook_url
        self.sign_secret = sign_secret
        self.dashboard_url = dashboard_url
        self.timeout_seconds = timeout_seconds

    async def send(self, opportunity: Opportunity) -> dict[str, Any]:
        return await asyncio.to_thread(self._send_sync, opportunity)

    def _send_sync(self, opportunity: Opportunity) -> dict[str, Any]:
        payload = self._build_payload(opportunity)
        if self.sign_secret:
            timestamp = str(int(time.time()))
            payload["timestamp"] = timestamp
            payload["sign"] = self._sign(timestamp)

        request = urllib.request.Request(
            self.webhook_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            body = response.read().decode("utf-8")
            result = json.loads(body or "{}")
            if response.getcode() >= 400 or result.get("code", 0) not in (0, None):
                raise RuntimeError(f"Lark webhook failed: {result}")
            return result

    def _build_payload(self, opportunity: Opportunity) -> dict[str, Any]:
        o = opportunity
        profit_100u = o.net_bps / 10000 * 100
        title = f"套利机会 {o.symbol}"
        direction = f"买入 {o.buy_exchange.upper()} / 卖出 {o.sell_exchange.upper()}"
        content = [
            [{"tag": "text", "text": direction}],
            [
                {
                    "tag": "text",
                    "text": f"净价差 {_fmt(o.net_bps)} bps | 毛价差 {_fmt(o.gross_spread)} USDT",
                }
            ],
            [
                {
                    "tag": "text",
                    "text": (
                        f"买价 {_fmt(o.buy_price)} | 卖价 {_fmt(o.sell_price)} | "
                        f"数量 {_fmt(o.executable_size)}"
                    ),
                }
            ],
            [
                {
                    "tag": "text",
                    "text": (
                        f"手续费 {_fmt(o.fee_bps)} bps | 100U利润 {_fmt(profit_100u)} USDT"
                    ),
                }
            ],
            [
                {
                    "tag": "text",
                    "text": f"时间 {o.observed_at}",
                }
            ],
        ]
        if self.dashboard_url:
            content.append(
                [
                    {
                        "tag": "a",
                        "text": "打开监控看板",
                        "href": self.dashboard_url,
                    }
                ]
            )

        return {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": content,
                    }
                }
            },
        }

    def _sign(self, timestamp: str) -> str:
        secret = f"{timestamp}\n{self.sign_secret}".encode("utf-8")
        digest = hmac.new(secret, digestmod=hashlib.sha256)
        return base64.b64encode(digest.digest()).decode("utf-8")
