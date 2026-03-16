from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.asyncio.client import ClientConnection

DEFAULT_PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
DEFAULT_DEMO_PUBLIC_URL = "wss://wspap.okx.com:8443/ws/v5/public"


@dataclass(slots=True, frozen=True)
class Subscription:
    channel: str
    inst_id: str

    def to_okx_arg(self) -> dict[str, str]:
        return {"channel": self.channel, "instId": self.inst_id}


class OKXPublicWebSocketClient:
    def __init__(
        self,
        subscription: Subscription,
        *,
        url: str = DEFAULT_PUBLIC_URL,
        heartbeat_interval: float = 20.0,
        reconnect_delay: float = 3.0,
        pretty: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.subscription = subscription
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.pretty = pretty
        self.logger = logger or logging.getLogger(__name__)
        self._last_message_at = 0.0
        self._awaiting_pong_since: float | None = None

    async def run(self, *, max_messages: int | None = None) -> None:
        received_data_messages = 0

        while True:
            try:
                self.logger.info("Connecting to %s", self.url)
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    ping_timeout=None,
                    open_timeout=10,
                    close_timeout=5,
                    max_queue=1024,
                ) as websocket:
                    self._last_message_at = time.monotonic()
                    self._awaiting_pong_since = None

                    await self._subscribe(websocket)
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(websocket))

                    try:
                        async for raw_message in websocket:
                            self._last_message_at = time.monotonic()

                            if raw_message == "pong":
                                self._awaiting_pong_since = None
                                self.logger.debug("Received pong from OKX")
                                continue

                            if raw_message == "ping":
                                await websocket.send("pong")
                                self.logger.debug("Replied pong to OKX")
                                continue

                            try:
                                message = json.loads(raw_message)
                            except json.JSONDecodeError:
                                self.logger.warning("Received non-JSON message: %s", raw_message)
                                continue

                            is_data_message = self._handle_message(message)
                            if is_data_message:
                                received_data_messages += 1
                                if max_messages is not None and received_data_messages >= max_messages:
                                    self.logger.info(
                                        "Received %s data messages, stopping",
                                        max_messages,
                                    )
                                    return
                    finally:
                        heartbeat_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await heartbeat_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Connection dropped: %s", exc)
                self.logger.info("Reconnecting in %.1f seconds", self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

    async def _subscribe(self, websocket: ClientConnection) -> None:
        payload = {
            "id": uuid.uuid4().hex[:8],
            "op": "subscribe",
            "args": [self.subscription.to_okx_arg()],
        }
        await websocket.send(json.dumps(payload))
        self.logger.info(
            "Subscribed request sent: channel=%s instId=%s",
            self.subscription.channel,
            self.subscription.inst_id,
        )

    async def _heartbeat_loop(self, websocket: ClientConnection) -> None:
        while True:
            await asyncio.sleep(self.heartbeat_interval)

            idle_seconds = time.monotonic() - self._last_message_at
            if idle_seconds < self.heartbeat_interval:
                continue

            if self._awaiting_pong_since is None:
                await websocket.send("ping")
                self._awaiting_pong_since = time.monotonic()
                self.logger.debug("Sent ping to OKX after %.1f seconds idle", idle_seconds)
                continue

            waited = time.monotonic() - self._awaiting_pong_since
            if waited >= self.heartbeat_interval:
                self.logger.warning("Pong timeout after %.1f seconds, closing connection", waited)
                await websocket.close(code=1011, reason="OKX pong timeout")
                return

    def _handle_message(self, message: dict[str, Any]) -> bool:
        event = message.get("event")
        if event == "subscribe":
            self.logger.info("Subscription confirmed by OKX: %s", json.dumps(message, ensure_ascii=False))
            return False

        if event == "notice":
            raise ConnectionError(message.get("msg", "OKX sent a notice"))

        if event == "error":
            code = message.get("code", "unknown")
            msg = message.get("msg", "unknown error")
            raise RuntimeError(f"OKX subscription error {code}: {msg}")

        if "arg" in message and "data" in message:
            print(self._format_message(message))
            return True

        self.logger.info("Control message: %s", json.dumps(message, ensure_ascii=False))
        return False

    def _format_message(self, message: dict[str, Any]) -> str:
        if self.pretty:
            return json.dumps(message, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"))
