#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from binance_ws import BinanceBookTickerWebSocketClient, DEFAULT_STREAM_URL  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subscribe to Binance public top-of-book market data over WebSocket",
    )
    parser.add_argument(
        "--symbol",
        default="BTCUSDT",
        help="Binance symbol, for example BTCUSDT",
    )
    parser.add_argument(
        "--url",
        default=DEFAULT_STREAM_URL,
        help="Override Binance WebSocket base URL",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after receiving this many data messages, 0 means run forever",
    )
    parser.add_argument(
        "--reconnect-delay",
        type=float,
        default=3.0,
        help="Reconnect delay in seconds after a disconnect",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print JSON payloads instead of one line per message",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    max_messages = args.max_messages if args.max_messages > 0 else None

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    client = BinanceBookTickerWebSocketClient(
        args.symbol,
        url=args.url,
        reconnect_delay=args.reconnect_delay,
        pretty=args.pretty,
    )
    await client.run(max_messages=max_messages)


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
