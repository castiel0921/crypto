#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from binance_rest import BinanceRestPoller  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poll Binance public market data over REST",
    )
    parser.add_argument(
        "--symbol",
        default="BTC/USDT",
        help="Trading pair, for example BTC/USDT or BTCUSDT",
    )
    parser.add_argument(
        "--kind",
        default="ticker",
        choices=["ticker", "order-book", "trades"],
        help="REST resource to poll",
    )
    parser.add_argument(
        "--base-url",
        default="https://data-api.binance.vision",
        help="Binance public market data base URL",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Polling interval in seconds",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Depth or trade count for order-book and trades",
    )
    parser.add_argument(
        "--max-polls",
        type=int,
        default=0,
        help="Stop after this many polls, 0 means run forever",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=3.0,
        help="Retry delay in seconds after a failed request",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="HTTP timeout in seconds",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print JSON payloads instead of one line per poll",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    max_polls = args.max_polls if args.max_polls > 0 else None

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    poller = BinanceRestPoller(
        symbol=args.symbol,
        kind=args.kind,
        base_url=args.base_url,
        interval_seconds=args.interval,
        limit=args.limit,
        retry_delay_seconds=args.retry_delay,
        timeout_seconds=args.timeout_seconds,
        pretty=args.pretty,
    )

    try:
        poller.run(max_polls=max_polls)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
