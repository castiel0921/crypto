#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from arbitrage import ArbitrageMonitor, parse_binance_book_ticker, parse_okx_books5  # noqa: E402
from binance_ws import BinanceBookTickerWebSocketClient  # noqa: E402
from dashboard import DashboardStore, start_dashboard_server  # noqa: E402
from notifications import LarkNotifier  # noqa: E402
from okx_ws import OKXPublicWebSocketClient, Subscription  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Monitor Binance and OKX top-of-book spread and emit arbitrage alerts",
    )
    parser.add_argument(
        "--symbol",
        default="BTC-USDT",
        help="Spot symbol in OKX style, for example BTC-USDT",
    )
    parser.add_argument(
        "--binance-url",
        default="wss://data-stream.binance.vision/ws",
        help="Binance WebSocket base URL",
    )
    parser.add_argument(
        "--okx-url",
        default="wss://ws.okx.com:8443/ws/v5/public",
        help="OKX public WebSocket URL",
    )
    parser.add_argument(
        "--binance-fee-bps",
        type=float,
        default=0.0,
        help="Assumed Binance taker fee in basis points",
    )
    parser.add_argument(
        "--okx-fee-bps",
        type=float,
        default=0.0,
        help="Assumed OKX taker fee in basis points",
    )
    parser.add_argument(
        "--min-net-bps",
        type=float,
        default=0.0,
        help="Only emit alerts when net spread reaches this threshold",
    )
    parser.add_argument(
        "--min-size",
        type=float,
        default=0.0,
        help="Minimum executable size at the top of book",
    )
    parser.add_argument(
        "--max-quote-age",
        type=float,
        default=2.0,
        help="Ignore quotes older than this many seconds",
    )
    parser.add_argument(
        "--alert-cooldown",
        type=float,
        default=5.0,
        help="Minimum seconds between alerts for the same direction",
    )
    parser.add_argument(
        "--webhook-url",
        default="",
        help="Optional webhook URL for POSTing alert payloads",
    )
    parser.add_argument(
        "--lark-webhook-url",
        default=os.environ.get("LARK_WEBHOOK_URL", ""),
        help="Lark custom bot webhook URL",
    )
    parser.add_argument(
        "--lark-sign-secret",
        default=os.environ.get("LARK_SIGN_SECRET", ""),
        help="Optional Lark custom bot signing secret",
    )
    parser.add_argument(
        "--dashboard-host",
        default=os.environ.get("DASHBOARD_HOST", "127.0.0.1"),
        help="Dashboard listen host",
    )
    parser.add_argument(
        "--dashboard-port",
        type=int,
        default=int(os.environ.get("DASHBOARD_PORT", "8080")),
        help="Dashboard listen port",
    )
    parser.add_argument(
        "--dashboard-public-url",
        default=os.environ.get("DASHBOARD_PUBLIC_URL", ""),
        help="Public dashboard URL for alert links",
    )
    parser.add_argument(
        "--disable-dashboard",
        action="store_true",
        help="Disable the embedded dashboard server",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty print alert JSON",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def okx_to_binance_symbol(symbol: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", symbol).upper()


async def async_main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    dashboard_store = None
    dashboard_runner = None
    if not args.disable_dashboard:
        dashboard_store = DashboardStore(
            symbol=args.symbol,
            binance_fee_bps=args.binance_fee_bps,
            okx_fee_bps=args.okx_fee_bps,
            min_net_bps=args.min_net_bps,
            min_size=args.min_size,
            max_quote_age_seconds=args.max_quote_age,
            lark_enabled=bool(args.lark_webhook_url),
        )
        dashboard_runner = await start_dashboard_server(
            dashboard_store,
            host=args.dashboard_host,
            port=args.dashboard_port,
            logger=logger,
        )

    lark_notifier = None
    if args.lark_webhook_url:
        lark_notifier = LarkNotifier(
            args.lark_webhook_url,
            sign_secret=args.lark_sign_secret,
            dashboard_url=args.dashboard_public_url,
        )

    monitor = ArbitrageMonitor(
        symbol=args.symbol,
        binance_fee_bps=args.binance_fee_bps,
        okx_fee_bps=args.okx_fee_bps,
        min_net_bps=args.min_net_bps,
        min_size=args.min_size,
        max_quote_age_seconds=args.max_quote_age,
        alert_cooldown_seconds=args.alert_cooldown,
        webhook_url=args.webhook_url,
        opportunity_handler=None,
        pretty=args.pretty,
    )

    async def handle_opportunity(opportunity) -> None:
        if dashboard_store is not None:
            await dashboard_store.record_opportunity(opportunity)

        if lark_notifier is None:
            return

        try:
            await lark_notifier.send(opportunity)
            if dashboard_store is not None:
                await dashboard_store.record_lark_delivery(ok=True, detail="Delivered to Lark")
        except Exception as exc:
            logger.warning("Failed to deliver Lark alert: %s", exc)
            if dashboard_store is not None:
                await dashboard_store.record_lark_delivery(ok=False, detail=str(exc))

    monitor.opportunity_handler = handle_opportunity

    async def handle_binance(message: dict[str, object]) -> None:
        quote = parse_binance_book_ticker(message)
        if dashboard_store is not None:
            await dashboard_store.record_quote(quote)
        await monitor.update_quote(quote)

    async def handle_okx(message: dict[str, object]) -> None:
        quote = parse_okx_books5(message)
        if dashboard_store is not None:
            await dashboard_store.record_quote(quote)
        await monitor.update_quote(quote)

    binance_client = BinanceBookTickerWebSocketClient(
        okx_to_binance_symbol(args.symbol),
        url=args.binance_url,
        reconnect_delay=3.0,
        print_messages=False,
        message_handler=handle_binance,
    )
    okx_client = OKXPublicWebSocketClient(
        Subscription(channel="books5", inst_id=args.symbol),
        url=args.okx_url,
        reconnect_delay=3.0,
        print_messages=False,
        message_handler=handle_okx,
    )

    try:
        await asyncio.gather(
            binance_client.run(),
            okx_client.run(),
        )
    finally:
        if dashboard_runner is not None:
            await dashboard_runner.cleanup()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
