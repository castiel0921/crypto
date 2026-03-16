from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests

DEFAULT_BASE_URL = "https://data-api.binance.vision"


def normalize_symbol(symbol: str) -> str:
    return symbol.replace("/", "").replace("-", "").replace("_", "").upper()


@dataclass(slots=True, frozen=True)
class PollResult:
    kind: str
    symbol: str
    payload: dict[str, Any] | list[dict[str, Any]]
    fetched_at: str

    def to_json(self, *, pretty: bool = False) -> str:
        body = {
            "exchange": "binance",
            "kind": self.kind,
            "symbol": self.symbol,
            "fetchedAt": self.fetched_at,
            "data": self.payload,
        }
        if pretty:
            return json.dumps(body, ensure_ascii=False, indent=2, sort_keys=True)
        return json.dumps(body, ensure_ascii=False, separators=(",", ":"))


class BinanceRestPoller:
    def __init__(
        self,
        *,
        symbol: str,
        kind: str,
        base_url: str = DEFAULT_BASE_URL,
        interval_seconds: float = 2.0,
        limit: int = 5,
        retry_delay_seconds: float = 3.0,
        timeout_seconds: float = 10.0,
        pretty: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        self.symbol = symbol
        self.binance_symbol = normalize_symbol(symbol)
        self.kind = kind
        self.base_url = base_url.rstrip("/")
        self.interval_seconds = interval_seconds
        self.limit = limit
        self.retry_delay_seconds = retry_delay_seconds
        self.timeout_seconds = timeout_seconds
        self.pretty = pretty
        self.logger = logger or logging.getLogger(__name__)
        self.session = requests.Session()

    def run(self, *, max_polls: int | None = None) -> None:
        completed_polls = 0
        self.logger.info("Polling Binance market data from %s", self.base_url)

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
            payload = self._request(
                "/api/v3/ticker/24hr",
                params={"symbol": self.binance_symbol},
            )
        elif self.kind == "order-book":
            payload = self._request(
                "/api/v3/depth",
                params={"symbol": self.binance_symbol, "limit": self.limit},
            )
        elif self.kind == "trades":
            payload = self._request(
                "/api/v3/trades",
                params={"symbol": self.binance_symbol, "limit": self.limit},
            )
        else:
            raise ValueError(f"Unsupported kind: {self.kind}")

        return PollResult(
            kind=self.kind,
            symbol=self.symbol,
            payload=payload,
            fetched_at=fetched_at,
        )

    def _request(self, path: str, *, params: dict[str, Any]) -> dict[str, Any] | list[dict[str, Any]]:
        response = self.session.get(
            f"{self.base_url}{path}",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()
