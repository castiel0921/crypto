from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import time
import urllib.request
from typing import Any

from arbitrage import Opportunity


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
        title = f"套利机会 {opportunity.symbol}"
        direction = f"买入 {opportunity.buy_exchange.upper()} / 卖出 {opportunity.sell_exchange.upper()}"
        content = [
            [{"tag": "text", "text": direction}],
            [
                {
                    "tag": "text",
                    "text": (
                        f"净价差 {opportunity.net_bps:.3f} bps | 毛价差 {opportunity.gross_spread:.2f} USDT"
                    ),
                }
            ],
            [
                {
                    "tag": "text",
                    "text": (
                        f"买价 {opportunity.buy_price:.2f} | 卖价 {opportunity.sell_price:.2f} | "
                        f"数量 {opportunity.executable_size:.6f}"
                    ),
                }
            ],
            [
                {
                    "tag": "text",
                    "text": f"手续费 {opportunity.fee_bps:.2f} bps | 时间 {opportunity.observed_at}",
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
