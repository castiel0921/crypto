from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import asdict
from pathlib import Path
from typing import Any

from aiohttp import web

from arbitrage import BestQuote, Opportunity

STATIC_DIR = Path(__file__).resolve().parent / "static"


def _iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


class DashboardStore:
    def __init__(
        self,
        *,
        market_types: list[str],
        binance_fee_bps: float,
        okx_fee_bps: float,
        min_net_bps: float,
        min_size: float,
        min_notional: float,
        max_quote_age_seconds: float,
        lark_enabled: bool,
        max_opportunities: int = 500,
        quote_refresh_interval: float = 0.5,
        top_spreads_limit: int = 50,
    ) -> None:
        self.market_types = market_types
        self.binance_fee_bps = binance_fee_bps
        self.okx_fee_bps = okx_fee_bps
        self.min_net_bps = min_net_bps
        self.min_size = min_size
        self.min_notional = min_notional
        self.max_quote_age_seconds = max_quote_age_seconds
        self.lark_enabled = lark_enabled
        self.started_at = _iso_now()
        self.total_opportunities = 0
        self.recent_opportunities: deque[dict[str, Any]] = deque(maxlen=max_opportunities)
        # quotes keyed by (canonical_symbol, exchange)
        self.latest_quotes: dict[tuple[str, str], dict[str, Any]] = {}
        self.active_symbols: set[str] = set()
        self.lark_status: dict[str, Any] = {
            "enabled": lark_enabled,
            "lastStatus": "idle" if lark_enabled else "disabled",
            "lastMessage": "",
            "lastAttemptAt": None,
        }
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._quote_refresh_interval = quote_refresh_interval
        self._last_snapshot_push = 0.0
        self._top_spreads_limit = top_spreads_limit

    async def record_quote(self, quote: BestQuote) -> None:
        key = (quote.symbol, quote.exchange)
        self.latest_quotes[key] = {
            "exchange": quote.exchange,
            "symbol": quote.symbol,
            "bidPrice": quote.bid_price,
            "bidSize": quote.bid_size,
            "askPrice": quote.ask_price,
            "askSize": quote.ask_size,
            "exchangeTsMs": quote.exchange_ts_ms,
            "updatedAt": _iso_now(),
        }
        self.active_symbols.add(quote.symbol)

        now = time.monotonic()
        if now - self._last_snapshot_push >= self._quote_refresh_interval:
            self._last_snapshot_push = now
            await self.broadcast_snapshot()

    async def record_opportunity(self, opportunity: Opportunity) -> None:
        self.total_opportunities += 1
        self.recent_opportunities.appendleft(asdict(opportunity))
        await self.broadcast_snapshot()

    async def record_lark_delivery(self, *, ok: bool, detail: str) -> None:
        self.lark_status = {
            "enabled": self.lark_enabled,
            "lastStatus": "ok" if ok else "error",
            "lastMessage": detail,
            "lastAttemptAt": _iso_now(),
        }
        await self.broadcast_snapshot()

    def snapshot(self) -> dict[str, Any]:
        return {
            "startedAt": self.started_at,
            "marketTypes": self.market_types,
            "stats": {
                "totalOpportunities": self.total_opportunities,
                "subscriberCount": len(self._subscribers),
                "activeSymbols": len(self.active_symbols),
            },
            "config": {
                "binanceFeeBps": self.binance_fee_bps,
                "okxFeeBps": self.okx_fee_bps,
                "minNetBps": self.min_net_bps,
                "minSize": self.min_size,
                "minNotional": self.min_notional,
                "maxQuoteAgeSeconds": self.max_quote_age_seconds,
            },
            "topSpreads": self._build_top_spreads(),
            "recentOpportunities": list(self.recent_opportunities),
            "delivery": {
                "lark": self.lark_status,
            },
        }

    def _build_top_spreads(self) -> list[dict[str, Any]]:
        """Compute top spreads across all symbol pairs, sorted by abs net_bps desc."""
        # Group quotes by canonical symbol
        symbols_with_both: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
        for (symbol, exchange), quote in self.latest_quotes.items():
            if symbol not in symbols_with_both:
                symbols_with_both[symbol] = (None, None)  # type: ignore[assignment]
            binance_q, okx_q = symbols_with_both[symbol]
            if exchange == "binance":
                symbols_with_both[symbol] = (quote, okx_q)
            elif exchange == "okx":
                symbols_with_both[symbol] = (binance_q, quote)

        spreads: list[dict[str, Any]] = []
        for symbol, (binance_q, okx_q) in symbols_with_both.items():
            if binance_q is None or okx_q is None:
                continue
            market_type = self._infer_market_type(symbol)
            for buy_q, sell_q, buy_fee, sell_fee in [
                (binance_q, okx_q, self.binance_fee_bps, self.okx_fee_bps),
                (okx_q, binance_q, self.okx_fee_bps, self.binance_fee_bps),
            ]:
                view = self._spread_view(
                    symbol=symbol,
                    market_type=market_type,
                    buy_quote=buy_q,
                    sell_quote=sell_q,
                    buy_fee_bps=buy_fee,
                    sell_fee_bps=sell_fee,
                )
                if view["meetsThreshold"]:
                    spreads.append(view)

        spreads.sort(key=lambda s: s["netBps"], reverse=True)
        return spreads[: self._top_spreads_limit]

    @staticmethod
    def _infer_market_type(symbol: str) -> str:
        if symbol.endswith("-SWAP"):
            if "-USDT-" in symbol:
                return "usdt_perp"
            return "coin_perp"
        return "spot"

    def _spread_view(
        self,
        *,
        symbol: str,
        market_type: str,
        buy_quote: dict[str, Any],
        sell_quote: dict[str, Any],
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> dict[str, Any]:
        buy_price = float(buy_quote["askPrice"])
        sell_price = float(sell_quote["bidPrice"])
        executable_size = min(float(buy_quote["askSize"]), float(sell_quote["bidSize"]))
        notional = executable_size * buy_price
        gross_spread = sell_price - buy_price
        gross_bps = gross_spread / buy_price * 10_000 if buy_price > 0 else 0.0
        fee_bps = buy_fee_bps + sell_fee_bps
        net_bps = gross_bps - fee_bps
        return {
            "symbol": symbol,
            "marketType": market_type,
            "buyExchange": buy_quote["exchange"],
            "sellExchange": sell_quote["exchange"],
            "buyPrice": buy_price,
            "sellPrice": sell_price,
            "executableSize": executable_size,
            "notional": notional,
            "grossSpread": gross_spread,
            "grossBps": gross_bps,
            "netBps": net_bps,
            "feeBps": fee_bps,
            "meetsThreshold": (
                executable_size >= self.min_size
                and notional >= self.min_notional
                and net_bps >= self.min_net_bps
            ),
        }

    async def broadcast_snapshot(self) -> None:
        snapshot = self.snapshot()
        dead_subscribers: list[asyncio.Queue[dict[str, Any]]] = []
        for subscriber in list(self._subscribers):
            try:
                if subscriber.full():
                    subscriber.get_nowait()
                subscriber.put_nowait(snapshot)
            except Exception:
                dead_subscribers.append(subscriber)

        for subscriber in dead_subscribers:
            self._subscribers.discard(subscriber)

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=5)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        self._subscribers.discard(queue)


async def handle_index(_: web.Request) -> web.FileResponse:
    return web.FileResponse(STATIC_DIR / "index.html")


async def handle_state(request: web.Request) -> web.Response:
    store: DashboardStore = request.app["store"]
    return web.json_response(store.snapshot())


async def handle_sse(request: web.Request) -> web.StreamResponse:
    store: DashboardStore = request.app["store"]
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
    await response.prepare(request)

    queue = store.subscribe()
    await response.write(_encode_sse(store.snapshot()))

    try:
        while True:
            try:
                payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                await response.write(_encode_sse(payload))
            except asyncio.TimeoutError:
                await response.write(b": ping\n\n")
    except (ConnectionResetError, asyncio.CancelledError, RuntimeError):
        pass
    finally:
        store.unsubscribe(queue)

    return response


def create_dashboard_app(store: DashboardStore) -> web.Application:
    app = web.Application()
    app["store"] = store
    app.router.add_get("/", handle_index)
    app.router.add_get("/api/state", handle_state)
    app.router.add_get("/api/events", handle_sse)
    app.router.add_static("/static", STATIC_DIR)
    return app


async def start_dashboard_server(
    store: DashboardStore,
    *,
    host: str,
    port: int,
    logger: logging.Logger | None = None,
) -> web.AppRunner:
    app = create_dashboard_app(store)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()

    if logger is not None:
        logger.info("Dashboard listening on http://%s:%s", host, port)

    return runner


def _encode_sse(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return f"event: snapshot\ndata: {body}\n\n".encode("utf-8")
