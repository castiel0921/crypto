from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import ccxt


@dataclass(slots=True, frozen=True)
class PollResult:
    kind: str
    symbol: str
    payload: dict[str, Any]
    fetched_at: str

    def to_json(self, *, pretty: bool = False) -> str:
        body = {
            "exchange": "okx",
            "kind": self.kind,
            "symbol": self.symbol,
            "fetchedAt": self.fetched_at,
            "data": self.payload,
        }
        if pretty:
            return json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


class OKXRestPoller:
    def __init__(
        self,
        *,
        symbol: str,
        kind: str,
        hostname: str = "www.okx.com",
        interval_seconds: float = 2.0,
        limit: int = 5,
        retry_delay_seconds: float = 3.0,
        timeout_ms: int = 10000,
        pretty: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.symbol = symbol
        self.kind = kind
        self.hostname = hostname
        self.interval_seconds = interval_seconds
        self.limit = limit
        self.retry_delay_seconds = retry_delay_seconds
        self.timeout_ms = timeout_ms
        self.pretty = pretty
        self.logger = logger or logging.getLogger(__name__)
        self.exchange = ccxt.okx(
            {
                "enableRateLimit": True,
                "hostname": self.hostname,
                "timeout": self.timeout_ms,
            }
        )

    def run(self, *, max_polls: int | None = None) -> None:
        completed_polls = 0
        self.logger.info("Loading OKX markets")
        self.exchange.load_markets()

        while True:
            try:
                result = self.poll_once()
                print(result.to_json(pretty=self.pretty))
                completed_polls += 1

                if max_polls is not None and completed_polls >= max_polls:
                    self.logger.info("Completed %s polls, stopping", max_polls)
                    return

                time.sleep(self.interval_seconds)
            except KeyboardInterrupt:
                raise
            except Exception as exc:
                self.logger.warning("Poll failed: %s", exc)
                self.logger.info("Retrying in %.1f seconds", self.retry_delay_seconds)
                time.sleep(self.retry_delay_seconds)

    def poll_once(self) -> PollResult:
        fetched_at = datetime.now(timezone.utc).isoformat()

        if self.kind == "ticker":
            payload = self.exchange.fetch_ticker(self.symbol)
        elif self.kind == "order-book":
            payload = self.exchange.fetch_order_book(self.symbol, limit=self.limit)
        elif self.kind == "trades":
            payload = {"trades": self.exchange.fetch_trades(self.symbol, limit=self.limit)}
        else:
            raise ValueError(f"Unsupported kind: {self.kind}")

        return PollResult(
            kind=self.kind,
            symbol=self.symbol,
            payload=payload,
            fetched_at=fetched_at,
        )
