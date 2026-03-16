from __future__ import annotations

import asyncio
import inspect
import json
import logging
import re
from typing import Any, Awaitable, Callable

import websockets

DEFAULT_STREAM_URL = "wss://data-stream.binance.vision/ws"
MessageHandler = Callable[[dict[str, Any]], Awaitable[None] | None]


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
        if message.get("e") != "bookTicker":
            self.logger.info("Control message: %s", json.dumps(message, ensure_ascii=False))
            return False

        if self.message_handler is not None:
            result = self.message_handler(message)
            if inspect.isawaitable(result):
                await result

        if self.print_messages:
            print(self._format_message(message))
        return True

    def _format_message(self, message: dict[str, Any]) -> str:
        if self.pretty:
            return json.dumps(message, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(message, ensure_ascii=False, separators=(",", ":"))
