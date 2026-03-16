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

from okx_ws import (  # noqa: E402
    DEFAULT_DEMO_PUBLIC_URL,
    DEFAULT_PUBLIC_URL,
    OKXPublicWebSocketClient,
    Subscription,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Subscribe to OKX public market data over WebSocket",
    )
    parser.add_argument(
        "--symbol",
        default="BTC-USDT",
        help="OKX instId to subscribe, for example BTC-USDT",
    )
    parser.add_argument(
        "--channel",
        default="tickers",
        choices=["tickers", "trades", "books5", "books"],
        help="OKX public channel to subscribe",
    )
    parser.add_argument(
        "--url",
        default="",
        help="Override WebSocket URL if your deployment needs a custom endpoint",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Use OKX demo trading public WebSocket endpoint",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=0,
        help="Stop after receiving this many data messages, 0 means run forever",
    )
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=20.0,
        help="Send text ping after this many idle seconds",
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


def resolve_url(args: argparse.Namespace) -> str:
    if args.url:
        return args.url
    if args.demo:
        return DEFAULT_DEMO_PUBLIC_URL
    return DEFAULT_PUBLIC_URL


async def async_main() -> None:
    args = parse_args()
    max_messages = args.max_messages if args.max_messages > 0 else None

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    client = OKXPublicWebSocketClient(
        Subscription(channel=args.channel, inst_id=args.symbol),
        url=resolve_url(args),
        heartbeat_interval=args.heartbeat_interval,
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
