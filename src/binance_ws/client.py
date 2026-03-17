from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from typing import Any, Awaitable, Callable, Union

import websockets

DEFAULT_STREAM_URL = "wss://data-stream.binance.vision/ws"
MessageHandler = Callable[[dict[str, Any]], Union[Awaitable[None], None]]


def normalize_symbol(symbol: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "", symbol).lower()


class BinanceBookTickerWebSocketClient:
    def __init__(
        self,
        symbol: str,
        *,
        url: str = DEFAULT_STREAM_URL,
        reconnect_delay: float = 3.0,
        pretty: bool = False,
        print_messages: bool = True,
        message_handler: MessageHandler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.symbol = symbol
        self.stream_symbol = normalize_symbol(symbol)
        self.url = url.rstrip("/")
        self.reconnect_delay = reconnect_delay
        self.pretty = pretty
        self.print_messages = print_messages
        self.message_handler = message_handler
        self.logger = logger or logging.getLogger(__name__)

    async def run(self, *, max_messages: int | None = None) -> None:
        received_messages = 0

        while True:
            try:
                stream_url = f"{self.url}/{self.stream_symbol}@bookTicker"
                self.logger.info("Connecting to %s", stream_url)
                async with websockets.connect(
                    stream_url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=10,
                    close_timeout=5,
                    max_queue=1024,
                ) as websocket:
                    async for raw_message in websocket:
                        try:
                            message = json.loads(raw_message)
                        except json.JSONDecodeError:
                            self.logger.warning("Received non-JSON message: %s", raw_message)
                            continue

                        is_data_message = await self._handle_message(message)
                        if is_data_message:
                            received_messages += 1
                            if max_messages is not None and received_messages >= max_messages:
                                self.logger.info("Received %s data messages, stopping", max_messages)
                                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Connection dropped: %s", exc)
                self.logger.info("Reconnecting in %.1f seconds", self.reconnect_delay)
                await asyncio.sleep(self.reconnect_delay)

    async def _handle_message(self, message: dict[str, Any]) -> bool:
        payload = message.get("data") if "data" in message else message
        if not isinstance(payload, dict) or not self._is_book_ticker_payload(payload):
            self.logger.info("Control message: %s", json.dumps(message, ensure_ascii=False))
            return False

        if self.message_handler is not None:
            result = self.message_handler(payload)
            if inspect.isawaitable(result):
                await result

        if self.print_messages:
            print(self._format_message(payload))
        return True

    def _format_message(self, message: dict[str, Any]) -> str:
        if self.pretty:
            return json.dumps(message, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"))

    @staticmethod
    def _is_book_ticker_payload(message: dict[str, Any]) -> bool:
        required_fields = {"u", "s", "b", "B", "a", "A"}
        return required_fields.issubset(message)


class BinanceMultiStreamClient:
    """Connect to Binance combined stream for multiple bookTicker streams."""

    MAX_STREAMS_PER_CONN = 200

    def __init__(
        self,
        streams: list[str],
        *,
        base_url: str = DEFAULT_STREAM_URL,
        reconnect_delay: float = 3.0,
        message_handler: MessageHandler | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.streams = streams
        self.base_url = base_url.rstrip("/").replace("/ws", "")
        self.reconnect_delay = reconnect_delay
        self.message_handler = message_handler
        self.logger = logger or logging.getLogger(__name__)

    async def run(self) -> None:
        chunks = [
            self.streams[i : i + self.MAX_STREAMS_PER_CONN]
            for i in range(0, len(self.streams), self.MAX_STREAMS_PER_CONN)
        ]
        self.logger.info(
            "Binance multi-stream: %d streams across %d connection(s)",
            len(self.streams),
            len(chunks),
        )
        await asyncio.gather(*(self._run_connection(chunk) for chunk in chunks))

    async def _run_connection(self, streams: list[str]) -> None:
        stream_path = "/".join(streams)
        url = f"{self.base_url}/stream?streams={stream_path}"

        while True:
            try:
                self.logger.info("Connecting to Binance combined stream (%d streams)", len(streams))
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    open_timeout=10,
                    close_timeout=5,
                    max_queue=4096,
                ) as ws:
                    async for raw in ws:
                        try:
                            message = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        data = message.get("data")
                        if not isinstance(data, dict):
                            continue

                        if not BinanceBookTickerWebSocketClient._is_book_ticker_payload(data):
                            continue

                        if self.message_handler is not None:
                            result = self.message_handler(data)
                            if inspect.isawaitable(result):
                                await result
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.warning("Binance combined stream dropped: %s", exc)
                await asyncio.sleep(self.reconnect_delay)
