#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

import aiohttp  # noqa: E402

from arbitrage import BestQuote, MultiArbitrageMonitor, parse_binance_book_ticker, parse_okx_books5  # noqa: E402
from binance_ws import BinanceMultiStreamClient  # noqa: E402
from dashboard import DashboardStore, ETFDailyDB, OIDailyDB, start_dashboard_server  # noqa: E402
from discovery import (  # noqa: E402
    MarketType,
    base_from_okx_symbol,
    binance_rest_base_url,
    binance_stream_name,
    binance_symbol,
    binance_ws_base_url,
    discover_common_pairs,
)
from notifications import LarkNotifier  # noqa: E402
from okx_ws import OKXMultiSubClient, Subscription  # noqa: E402

_OKX_OI_URL = "https://www.okx.com/api/v5/public/open-interest"
_SOSOVALUE_ETF_URL = "https://api.sosovalue.xyz/openapi/v2/etf/historicalInflowChart"


async def _fetch_json(session: aiohttp.ClientSession, url: str, **params: str) -> dict:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json()


async def poll_open_interest(
    store: DashboardStore,
    market_types: list[MarketType],
    all_pairs: dict[MarketType, list[str]],
    interval: float = 60.0,
    top_n: int = 20,
) -> None:
    """Periodically poll open interest from OKX and Binance, push top N to dashboard."""
    poll_logger = logging.getLogger("open_interest")

    # Only poll perpetual markets
    perp_types = [mt for mt in market_types if mt != MarketType.SPOT]
    if not perp_types:
        poll_logger.info("No perpetual markets configured, skipping OI polling")
        return

    while True:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                # Step 1: Fetch OKX OI (batch — one request per instType)
                okx_oi: dict[str, float] = {}  # canonical_symbol -> OI in USDT
                for mt in perp_types:
                    inst_type = "SWAP"
                    try:
                        data = await _fetch_json(session, _OKX_OI_URL, instType=inst_type)
                        for item in data.get("data", []):
                            inst_id = item.get("instId", "")
                            if inst_id not in {p for p in all_pairs.get(mt, [])}:
                                continue
                            # Use oiUsd directly from OKX API (already in USD)
                            oi_usd = float(item.get("oiUsd", 0))
                            if oi_usd > 0:
                                okx_oi[inst_id] = oi_usd
                    except Exception as exc:
                        poll_logger.warning("OKX OI fetch failed: %s", exc)

                # Step 2: Sort by OKX OI to find top symbols, then query Binance for those
                top_symbols = sorted(okx_oi.keys(), key=lambda s: okx_oi.get(s, 0), reverse=True)[:top_n]

                binance_oi: dict[str, float] = {}  # canonical_symbol -> OI in USDT
                for symbol in top_symbols:
                    base = base_from_okx_symbol(symbol)
                    if symbol.endswith("-USDT-SWAP"):
                        mt = MarketType.USDT_PERP
                    elif symbol.endswith("-USD-SWAP"):
                        mt = MarketType.COIN_PERP
                    else:
                        continue

                    bn_sym = binance_symbol(base, mt)
                    bn_base_url = binance_rest_base_url(mt)
                    if mt == MarketType.USDT_PERP:
                        oi_path = "/fapi/v1/openInterest"
                    else:
                        oi_path = "/dapi/v1/openInterest"

                    try:
                        data = await _fetch_json(session, f"{bn_base_url}{oi_path}", symbol=bn_sym)
                        oi_qty = float(data.get("openInterest", 0))
                        # Must have a price to convert coin -> USDT
                        quote = store.latest_quotes.get((symbol, "binance"))
                        if not quote:
                            continue
                        price = (float(quote["bidPrice"]) + float(quote["askPrice"])) / 2
                        if price <= 0:
                            continue
                        binance_oi[symbol] = oi_qty * price
                    except Exception as exc:
                        poll_logger.debug("Binance OI fetch for %s failed: %s", bn_sym, exc)

                    # Small delay to respect rate limits
                    await asyncio.sleep(0.1)

                # Step 3: Merge and sort
                result: list[dict] = []
                for symbol in top_symbols:
                    bn_val = binance_oi.get(symbol, 0)
                    okx_val = okx_oi.get(symbol, 0)
                    total = bn_val + okx_val
                    if symbol.endswith("-USDT-SWAP"):
                        mt_str = "usdt_perp"
                    elif symbol.endswith("-USD-SWAP"):
                        mt_str = "coin_perp"
                    else:
                        mt_str = "spot"
                    result.append({
                        "symbol": symbol,
                        "marketType": mt_str,
                        "binanceOI": bn_val,
                        "okxOI": okx_val,
                        "totalOI": total,
                    })
                result.sort(key=lambda x: x["totalOI"], reverse=True)

                await store.update_open_interest(result[:top_n])
                poll_logger.info("OI updated: %d symbols", len(result[:top_n]))

        except Exception as exc:
            poll_logger.warning("OI poll cycle failed: %s", exc)

        await asyncio.sleep(interval)


async def poll_oi_daily_history(
    store: DashboardStore,
    market_types: list[MarketType],
    all_pairs: dict[MarketType, list[str]],
    interval: float = 3600.0,
    top_n: int = 20,
) -> None:
    """Fetch daily OI history from Binance for top symbols, persisted to SQLite."""
    poll_logger = logging.getLogger("oi_history")

    perp_types = [mt for mt in market_types if mt != MarketType.SPOT]
    if not perp_types:
        return

    # Wait for real-time OI to populate first
    await asyncio.sleep(70)

    first_run = True
    while True:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                current_oi = store._open_interest
                top_symbols = [item["symbol"] for item in current_oi[:top_n]]

                history: dict[str, list[dict]] = {}

                for symbol in top_symbols:
                    base = base_from_okx_symbol(symbol)
                    if symbol.endswith("-USDT-SWAP"):
                        mt = MarketType.USDT_PERP
                    elif symbol.endswith("-USD-SWAP"):
                        mt = MarketType.COIN_PERP
                    else:
                        continue

                    # Incremental fetch: on first run fetch full 30 days,
                    # on subsequent runs only fetch missing days
                    limit = 30
                    if not first_run and store._oi_db is not None:
                        latest = store._oi_db.get_latest_date(symbol)
                        if latest is not None:
                            from datetime import date as _date
                            gap = (_date.today() - _date.fromisoformat(latest)).days
                            if gap <= 0:
                                continue  # already up to date
                            limit = min(gap + 1, 30)

                    bn_sym = binance_symbol(base, mt)
                    bn_base_url = binance_rest_base_url(mt)
                    hist_path = "/futures/data/openInterestHist"

                    try:
                        data = await _fetch_json(
                            session,
                            f"{bn_base_url}{hist_path}",
                            symbol=bn_sym,
                            period="1d",
                            limit=str(limit),
                        )
                        if isinstance(data, list):
                            points = []
                            for item in data:
                                ts = int(item.get("timestamp", 0))
                                val = float(item.get("sumOpenInterestValue", 0))
                                iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts / 1000))
                                points.append({"t": iso, "v": val})
                            history[symbol] = points
                        else:
                            poll_logger.warning("Binance OI history for %s unexpected: %s", bn_sym, str(data)[:200])
                    except Exception as exc:
                        poll_logger.warning("Binance OI history for %s failed: %s", bn_sym, exc)

                    await asyncio.sleep(0.5)

                await store.update_oi_daily_history(history)
                poll_logger.info("OI daily history updated: %d symbols", len(history))
                first_run = False

        except Exception as exc:
            poll_logger.warning("OI daily history poll failed: %s", exc)

        await asyncio.sleep(interval)


async def poll_etf_history(
    store: DashboardStore,
    api_key: str,
    interval: float = 3600.0,
) -> None:
    """Fetch ETF daily inflow data from SoSoValue, persisted to SQLite."""
    poll_logger = logging.getLogger("etf_history")
    etf_types = ["us-btc-spot", "us-eth-spot"]

    # Small delay to not compete with OI init
    await asyncio.sleep(10)

    while True:
        try:
            async with aiohttp.ClientSession(trust_env=True) as session:
                for etf_type in etf_types:
                    try:
                        async with session.post(
                            _SOSOVALUE_ETF_URL,
                            json={"type": etf_type},
                            headers={
                                "x-soso-api-key": api_key,
                                "Content-Type": "application/json",
                            },
                            timeout=aiohttp.ClientTimeout(total=15),
                        ) as resp:
                            resp.raise_for_status()
                            body = await resp.json()
                            data = body.get("data") or {}
                            records = data.get("list", []) if isinstance(data, dict) else data
                            if records:
                                await store.update_etf_history(etf_type, records)
                                poll_logger.info(
                                    "ETF %s updated: %d records, latest %s",
                                    etf_type, len(records), records[-1].get("date"),
                                )
                    except Exception as exc:
                        poll_logger.warning("ETF %s fetch failed: %s", etf_type, exc)

                    await asyncio.sleep(1)

        except Exception as exc:
            poll_logger.warning("ETF poll cycle failed: %s", exc)

        await asyncio.sleep(interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Multi-symbol, multi-market cross-exchange arbitrage monitor",
    )
    parser.add_argument(
        "--market-types",
        default="spot",
        help="Comma-separated market types: spot,usdt_perp,coin_perp (default: spot)",
    )
    parser.add_argument(
        "--max-pairs",
        type=int,
        default=0,
        help="Max pairs per market type (0 = all, useful for testing)",
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
        "--min-notional",
        type=float,
        default=0.0,
        help="Minimum notional value in USDT (size * price)",
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
        "--sosovalue-api-key",
        default=os.environ.get("SOSOVALUE_API_KEY", ""),
        help="SoSoValue API key for ETF data",
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


async def async_main() -> None:
    args = parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    logger = logging.getLogger(__name__)

    # Parse market types
    market_types = [MarketType(mt.strip()) for mt in args.market_types.split(",")]
    logger.info("Market types: %s", [mt.value for mt in market_types])

    # Discover common pairs for each market type
    all_pairs: dict[MarketType, list[str]] = {}
    binance_to_canonical: dict[str, str] = {}

    for mt in market_types:
        pairs = await discover_common_pairs(mt)
        if args.max_pairs > 0:
            pairs = pairs[: args.max_pairs]
        all_pairs[mt] = pairs
        logger.info("%s: %d pairs", mt.value, len(pairs))

        # Build reverse lookup: binance symbol -> canonical (OKX) symbol
        for canonical in pairs:
            base = base_from_okx_symbol(canonical)
            bn_stream = binance_stream_name(base, mt)
            # Stream name is like "btcusdt@bookTicker", the symbol in message is "BTCUSDT"
            bn_sym = bn_stream.split("@")[0].upper()
            binance_to_canonical[bn_sym] = canonical

    # Dashboard store
    dashboard_store = None
    dashboard_runner = None
    oi_db = None
    etf_db = None
    if not args.disable_dashboard:
        db_path = PROJECT_ROOT / "data" / "oi_daily.db"
        oi_db = OIDailyDB(db_path)
        etf_db_path = PROJECT_ROOT / "data" / "etf_daily.db"
        etf_db = ETFDailyDB(etf_db_path)
        logger.info("Databases opened at %s", PROJECT_ROOT / "data")

        dashboard_store = DashboardStore(
            market_types=[mt.value for mt in market_types],
            binance_fee_bps=args.binance_fee_bps,
            okx_fee_bps=args.okx_fee_bps,
            min_net_bps=args.min_net_bps,
            min_size=args.min_size,
            min_notional=args.min_notional,
            max_quote_age_seconds=args.max_quote_age,
            lark_enabled=bool(args.lark_webhook_url),
            oi_db=oi_db,
            etf_db=etf_db,
        )
        dashboard_runner = await start_dashboard_server(
            dashboard_store,
            host=args.dashboard_host,
            port=args.dashboard_port,
            logger=logger,
        )

    # Lark notifier
    lark_notifier = None
    if args.lark_webhook_url:
        lark_notifier = LarkNotifier(
            args.lark_webhook_url,
            sign_secret=args.lark_sign_secret,
            dashboard_url=args.dashboard_public_url,
        )

    # Multi-symbol arbitrage monitor
    monitor = MultiArbitrageMonitor(
        binance_fee_bps=args.binance_fee_bps,
        okx_fee_bps=args.okx_fee_bps,
        min_net_bps=args.min_net_bps,
        min_size=args.min_size,
        min_notional=args.min_notional,
        max_quote_age_seconds=args.max_quote_age,
        alert_cooldown_seconds=args.alert_cooldown,
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

    # Binance message handler: route by symbol
    async def handle_binance(message: dict[str, object]) -> None:
        bn_sym = str(message.get("s", ""))
        canonical = binance_to_canonical.get(bn_sym)
        if canonical is None:
            return
        raw = parse_binance_book_ticker(message)
        # Re-create with canonical symbol
        quote = BestQuote(
            exchange=raw.exchange,
            symbol=canonical,
            bid_price=raw.bid_price,
            bid_size=raw.bid_size,
            ask_price=raw.ask_price,
            ask_size=raw.ask_size,
            exchange_ts_ms=raw.exchange_ts_ms,
            received_at=raw.received_at,
        )
        if dashboard_store is not None:
            await dashboard_store.record_quote(quote)
        await monitor.update_quote(quote)

    # OKX message handler: route by instId
    async def handle_okx(message: dict[str, object]) -> None:
        quote = parse_okx_books5(message)
        if dashboard_store is not None:
            await dashboard_store.record_quote(quote)
        await monitor.update_quote(quote)

    # Build WebSocket clients for each market type
    ws_tasks: list[asyncio.Task[None]] = []

    for mt in market_types:
        pairs = all_pairs[mt]
        if not pairs:
            continue

        # Binance streams
        streams = [binance_stream_name(base_from_okx_symbol(p), mt) for p in pairs]
        binance_client = BinanceMultiStreamClient(
            streams,
            base_url=binance_ws_base_url(mt),
            reconnect_delay=3.0,
            message_handler=handle_binance,
        )

        # OKX subscriptions
        subs = [Subscription(channel="books5", inst_id=p) for p in pairs]
        okx_client = OKXMultiSubClient(
            subs,
            reconnect_delay=3.0,
            message_handler=handle_okx,
        )

        ws_tasks.append(asyncio.create_task(binance_client.run()))
        ws_tasks.append(asyncio.create_task(okx_client.run()))

    # Open interest polling
    if dashboard_store is not None:
        ws_tasks.append(asyncio.create_task(
            poll_open_interest(dashboard_store, market_types, all_pairs)
        ))
        ws_tasks.append(asyncio.create_task(
            poll_oi_daily_history(dashboard_store, market_types, all_pairs)
        ))
        # ETF data polling
        if args.sosovalue_api_key:
            ws_tasks.append(asyncio.create_task(
                poll_etf_history(dashboard_store, args.sosovalue_api_key)
            ))

    try:
        await asyncio.gather(*ws_tasks)
    finally:
        if dashboard_runner is not None:
            await dashboard_runner.cleanup()
        if oi_db is not None:
            oi_db.close()
        if etf_db is not None:
            etf_db.close()


def main() -> None:
    try:
        asyncio.run(async_main())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
