from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import uuid
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Union

import websockets
from websockets.asyncio.client import ClientConnection

DEFAULT_PUBLIC_URL = "wss://ws.okx.com:8443/ws/v5/public"
DEFAULT_DEMO_PUBLIC_URL = "wss://wspap.okx.com:8443/ws/v5/public"
MessageHandler = Callable[[dict[str, Any]], Union[Awaitable[None], None]]


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
        print_messages: bool = True,
        message_handler: MessageHandler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.subscription = subscription
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.pretty = pretty
        self.print_messages = print_messages
        self.message_handler = message_handler
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

                            is_data_message = await self._handle_message(message)
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

    async def _handle_message(self, message: dict[str, Any]) -> bool:
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
            if self.message_handler is not None:
                result = self.message_handler(message)
                if inspect.isawaitable(result):
                    await result
            if self.print_messages:
                print(self._format_message(message))
            return True

        self.logger.info("Control message: %s", json.dumps(message, ensure_ascii=False))
        return False

    def _format_message(self, message: dict[str, Any]) -> str:
        if self.pretty:
            return json.dumps(message, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"))


class OKXMultiSubClient:
    """Connect to OKX public WebSocket with multiple subscriptions."""

    MAX_SUBS_PER_CONN = 100

    def __init__(
        self,
        subscriptions: list[Subscription],
        *,
        url: str = DEFAULT_PUBLIC_URL,
        heartbeat_interval: float = 20.0,
        reconnect_delay: float = 3.0,
        message_handler: MessageHandler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.subscriptions = subscriptions
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.message_handler = message_handler
        self.logger = logger or logging.getLogger(__name__)

    async def run(self) -> None:
        chunks = [
            self.subscriptions[i : i + self.MAX_SUBS_PER_CONN]
            for i in range(0, len(self.subscriptions), self.MAX_SUBS_PER_CONN)
        ]
        self.logger.info(
            "OKX multi-sub: %d subscriptions across %d connection(s)",
            len(self.subscriptions),
            len(chunks),
        )
        await asyncio.gather(*(self._run_connection(chunk) for chunk in chunks))

    async def _run_connection(self, subs: list[Subscription]) -> None:
        while True:
            try:
                self.logger.info("Connecting to OKX (%d subscriptions)", len(subs))
                async with websockets.connect(
                    self.url,
                    ping_interval=None,
                    ping_timeout=None,
                    open_timeout=10,
                    close_timeout=5,
                    max_queue=4096,
                ) as ws:
                    last_message_at = time.monotonic()

                    # Subscribe in batches of 20 args per message
                    for i in range(0, len(subs), 20):
                        batch = subs[i : i + 20]
                        payload = {
                            "id": uuid.uuid4().hex[:8],
                            "op": "subscribe",
                            "args": [s.to_okx_arg() for s in batch],
                        }
                        await ws.send(json.dumps(payload))
                    self.logger.info("Subscribed to %d OKX instruments", len(subs))

                    async def heartbeat() -> None:
                        nonlocal last_message_at
                        awaiting_pong_since: float | None = None
                        while True:
                            await asyncio.sleep(self.heartbeat_interval)
                            idle = time.monotonic() - last_message_at
                            if idle < self.heartbeat_interval:
                                continue
                            if awaiting_pong_since is None:
                                await ws.send("ping")
                                awaiting_pong_since = time.monotonic()
                                continue
                            waited = time.monotonic() - awaiting_pong_since
                            if waited >= self.heartbeat_interval:
                                await ws.close(code=1011, reason="OKX pong timeout")
                                return

                    hb_task = asyncio.create_task(heartbeat())
                    try:
                        async for raw in ws:
                            last_message_at = time.monotonic()

                            if raw == "pong":
                                continue
                            if raw == "ping":
                                await ws.send("pong")
                                continue

                            try:
                                message = json.loads(raw)
                            except json.JSONDecodeError:
                                continue

                            if "arg" in message and "data" in message:
                                if self.message_handler is not None:
                                    result = self.message_handler(message)
                                    if inspect.isawaitable(result):
                                        await result
                    finally:
                        hb_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await hb_task
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("OKX multi-sub connection dropped: %s", exc)
                await asyncio.sleep(self.reconnect_delay)
