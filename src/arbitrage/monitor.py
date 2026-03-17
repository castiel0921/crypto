from __future__ import annotations

import asyncio
import inspect
import json
import logging
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Union

OpportunityHandler = Callable[["Opportunity"], Union[Awaitable[None], None]]


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class BestQuote:
    exchange: str
    symbol: str
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    exchange_ts_ms: int | None
    received_at: float


@dataclass(slots=True, frozen=True)
class Opportunity:
    observed_at: str
    symbol: str
    market_type: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    executable_size: float
    gross_spread: float
    gross_bps: float
    net_bps: float
    fee_bps: float
    quotes: dict[str, dict[str, float | str | int | None]]

    def to_json(self, *, pretty: bool = False) -> str:
        body = asdict(self)
        if pretty:
            return json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


def parse_okx_books5(message: dict[str, Any], *, received_at: float | None = None) -> BestQuote:
    data = message["data"][0]
    best_bid = data["bids"][0]
    best_ask = data["asks"][0]
    return BestQuote(
        exchange="okx",
        symbol=message["arg"]["instId"],
        bid_price=float(best_bid[0]),
        bid_size=float(best_bid[1]),
        ask_price=float(best_ask[0]),
        ask_size=float(best_ask[1]),
        exchange_ts_ms=int(data["ts"]) if data.get("ts") is not None else None,
        received_at=received_at if received_at is not None else time.monotonic(),
    )


def parse_binance_book_ticker(message: dict[str, Any], *, received_at: float | None = None) -> BestQuote:
    return BestQuote(
        exchange="binance",
        symbol=message["s"],
        bid_price=float(message["b"]),
        bid_size=float(message["B"]),
        ask_price=float(message["a"]),
        ask_size=float(message["A"]),
        exchange_ts_ms=int(message["E"]) if message.get("E") is not None else None,
        received_at=received_at if received_at is not None else time.monotonic(),
    )


class ArbitrageMonitor:
    def __init__(
        self,
        *,
        symbol: str,
        binance_fee_bps: float = 0.0,
        okx_fee_bps: float = 0.0,
        min_net_bps: float = 0.0,
        min_size: float = 0.0,
        max_quote_age_seconds: float = 2.0,
        alert_cooldown_seconds: float = 5.0,
        webhook_url: str = "",
        opportunity_handler: OpportunityHandler | None = None,
        pretty: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.symbol = symbol
        self.binance_fee_bps = binance_fee_bps
        self.okx_fee_bps = okx_fee_bps
        self.min_net_bps = min_net_bps
        self.min_size = min_size
        self.max_quote_age_seconds = max_quote_age_seconds
        self.alert_cooldown_seconds = alert_cooldown_seconds
        self.webhook_url = webhook_url
        self.opportunity_handler = opportunity_handler
        self.pretty = pretty
        self.logger = logger or logging.getLogger(__name__)
        self._quotes: dict[str, BestQuote] = {}
        self._last_alert_at: dict[str, float] = {}

    async def update_quote(self, quote: BestQuote) -> None:
        self._quotes[quote.exchange] = quote

        for opportunity in self._evaluate():
            alert_key = f"{opportunity.buy_exchange}->{opportunity.sell_exchange}"
            now = time.monotonic()
            last_alert_at = self._last_alert_at.get(alert_key, 0.0)
            if now - last_alert_at < self.alert_cooldown_seconds:
                continue

            self._last_alert_at[alert_key] = now
            print(opportunity.to_json(pretty=self.pretty))

            if self.opportunity_handler is not None:
                result = self.opportunity_handler(opportunity)
                if inspect.isawaitable(result):
                    await result

            if self.webhook_url:
                await asyncio.to_thread(self._send_webhook, opportunity)

    def _evaluate(self) -> list[Opportunity]:
        binance_quote = self._fresh_quote("binance")
        okx_quote = self._fresh_quote("okx")
        if binance_quote is None or okx_quote is None:
            return []

        opportunities: list[Opportunity] = []

        binance_to_okx = self._build_opportunity(
            buy_quote=binance_quote,
            sell_quote=okx_quote,
            buy_fee_bps=self.binance_fee_bps,
            sell_fee_bps=self.okx_fee_bps,
        )
        if binance_to_okx is not None:
            opportunities.append(binance_to_okx)

        okx_to_binance = self._build_opportunity(
            buy_quote=okx_quote,
            sell_quote=binance_quote,
            buy_fee_bps=self.okx_fee_bps,
            sell_fee_bps=self.binance_fee_bps,
        )
        if okx_to_binance is not None:
            opportunities.append(okx_to_binance)

        return opportunities

    def _fresh_quote(self, exchange: str) -> BestQuote | None:
        quote = self._quotes.get(exchange)
        if quote is None:
            return None

        age = time.monotonic() - quote.received_at
        if age > self.max_quote_age_seconds:
            self.logger.debug("Skipping stale %s quote age=%.3fs", exchange, age)
            return None
        return quote

    def _build_opportunity(
        self,
        *,
        buy_quote: BestQuote,
        sell_quote: BestQuote,
        buy_fee_bps: float,
        sell_fee_bps: float,
    ) -> Opportunity | None:
        if buy_quote.ask_price <= 0 or sell_quote.bid_price <= 0:
            return None

        executable_size = min(buy_quote.ask_size, sell_quote.bid_size)
        if executable_size < self.min_size:
            return None

        gross_spread = sell_quote.bid_price - buy_quote.ask_price
        if gross_spread <= 0:
            return None

        gross_bps = gross_spread / buy_quote.ask_price * 10_000
        fee_bps = buy_fee_bps + sell_fee_bps
        net_bps = gross_bps - fee_bps
        if net_bps < self.min_net_bps:
            return None

        return Opportunity(
            observed_at=_iso_now(),
            symbol=self.symbol,
            market_type="spot",
            buy_exchange=buy_quote.exchange,
            sell_exchange=sell_quote.exchange,
            buy_price=buy_quote.ask_price,
            sell_price=sell_quote.bid_price,
            executable_size=executable_size,
            gross_spread=gross_spread,
            gross_bps=gross_bps,
            net_bps=net_bps,
            fee_bps=fee_bps,
            quotes={
                buy_quote.exchange: {
                    "symbol": buy_quote.symbol,
                    "bidPrice": buy_quote.bid_price,
                    "bidSize": buy_quote.bid_size,
                    "askPrice": buy_quote.ask_price,
                    "askSize": buy_quote.ask_size,
                    "exchangeTsMs": buy_quote.exchange_ts_ms,
                },
                sell_quote.exchange: {
                    "symbol": sell_quote.symbol,
                    "bidPrice": sell_quote.bid_price,
                    "bidSize": sell_quote.bid_size,
                    "askPrice": sell_quote.ask_price,
                    "askSize": sell_quote.ask_size,
                    "exchangeTsMs": sell_quote.exchange_ts_ms,
                },
            },
        )

    def _send_webhook(self, opportunity: Opportunity) -> None:
        payload = opportunity.to_json(pretty=False).encode("utf-8")
        request = urllib.request.Request(
            self.webhook_url,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()


class MultiArbitrageMonitor:
    def __init__(
        self,
        *,
        binance_fee_bps: float = 0.0,
        okx_fee_bps: float = 0.0,
        min_net_bps: float = 0.0,
        min_size: float = 0.0,
        max_quote_age_seconds: float = 2.0,
        alert_cooldown_seconds: float = 5.0,
        opportunity_handler: OpportunityHandler | None = None,
        pretty: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.binance_fee_bps = binance_fee_bps
        self.okx_fee_bps = okx_fee_bps
        self.min_net_bps = min_net_bps
        self.min_size = min_size
        self.max_quote_age_seconds = max_quote_age_seconds
        self.alert_cooldown_seconds = alert_cooldown_seconds
        self.opportunity_handler = opportunity_handler
        self.pretty = pretty
        self.logger = logger or logging.getLogger(__name__)
        self._quotes: dict[tuple[str, str], BestQuote] = {}
        self._last_alert_at: dict[tuple[str, str], float] = {}

    async def update_quote(self, quote: BestQuote) -> None:
        key = (quote.symbol, quote.exchange)
        self._quotes[key] = quote

        for opportunity in self._evaluate(quote.symbol):
            alert_key = (quote.symbol, f"{opportunity.buy_exchange}->{opportunity.sell_exchange}")
            now = time.monotonic()
            last = self._last_alert_at.get(alert_key, 0.0)
            if now - last < self.alert_cooldown_seconds:
                continue

            self._last_alert_at[alert_key] = now
            print(opportunity.to_json(pretty=self.pretty))

            if self.opportunity_handler is not None:
                result = self.opportunity_handler(opportunity)
                if inspect.isawaitable(result):
                    await result

    def _fresh_quote(self, symbol: str, exchange: str) -> BestQuote | None:
        quote = self._quotes.get((symbol, exchange))
        if quote is None:
            return None
        age = time.monotonic() - quote.received_at
        if age > self.max_quote_age_seconds:
            return None
        return quote

    def _evaluate(self, symbol: str) -> list[Opportunity]:
        binance_q = self._fresh_quote(symbol, "binance")
        okx_q = self._fresh_quote(symbol, "okx")
        if binance_q is None or okx_q is None:
            return []

        market_type = self._infer_market_type(symbol)
        results: list[Opportunity] = []

        opp = self._build_opportunity(binance_q, okx_q, self.binance_fee_bps, self.okx_fee_bps, symbol, market_type)
        if opp is not None:
            results.append(opp)

        opp = self._build_opportunity(okx_q, binance_q, self.okx_fee_bps, self.binance_fee_bps, symbol, market_type)
        if opp is not None:
            results.append(opp)

        return results

    @staticmethod
    def _infer_market_type(symbol: str) -> str:
        if symbol.endswith("-SWAP"):
            if "-USDT-" in symbol:
                return "usdt_perp"
            return "coin_perp"
        return "spot"

    def _build_opportunity(
        self,
        buy_q: BestQuote,
        sell_q: BestQuote,
        buy_fee_bps: float,
        sell_fee_bps: float,
        symbol: str,
        market_type: str,
    ) -> Opportunity | None:
        if buy_q.ask_price <= 0 or sell_q.bid_price <= 0:
            return None

        executable_size = min(buy_q.ask_size, sell_q.bid_size)
        if executable_size < self.min_size:
            return None

        gross_spread = sell_q.bid_price - buy_q.ask_price
        if gross_spread <= 0:
            return None

        gross_bps = gross_spread / buy_q.ask_price * 10_000
        fee_bps = buy_fee_bps + sell_fee_bps
        net_bps = gross_bps - fee_bps
        if net_bps < self.min_net_bps:
            return None

        return Opportunity(
            observed_at=_iso_now(),
            symbol=symbol,
            market_type=market_type,
            buy_exchange=buy_q.exchange,
            sell_exchange=sell_q.exchange,
            buy_price=buy_q.ask_price,
            sell_price=sell_q.bid_price,
            executable_size=executable_size,
            gross_spread=gross_spread,
            gross_bps=gross_bps,
            net_bps=net_bps,
            fee_bps=fee_bps,
            quotes={
                buy_q.exchange: {
                    "symbol": buy_q.symbol,
                    "bidPrice": buy_q.bid_price,
                    "bidSize": buy_q.bid_size,
                    "askPrice": buy_q.ask_price,
                    "askSize": buy_q.ask_size,
                    "exchangeTsMs": buy_q.exchange_ts_ms,
                },
                sell_q.exchange: {
                    "symbol": sell_q.symbol,
                    "bidPrice": sell_q.bid_price,
                    "bidSize": sell_q.bid_size,
                    "askPrice": sell_q.ask_price,
                    "askSize": sell_q.ask_size,
                    "exchangeTsMs": sell_q.exchange_ts_ms,
                },
            },
        )
