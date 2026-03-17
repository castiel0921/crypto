from __future__ import annotations

import logging
from typing import Any

import aiohttp

from discovery.symbols import (
    MarketType,
    binance_exchange_info_url,
    okx_symbol,
)

logger = logging.getLogger(__name__)

_OKX_INSTRUMENTS_URL = "https://www.okx.com/api/v5/public/instruments"


async def _fetch_json(session: aiohttp.ClientSession, url: str, **params: str) -> Any:
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
        resp.raise_for_status()
        return await resp.json()


def _extract_binance_bases(data: dict[str, Any], market_type: MarketType) -> set[str]:
    bases: set[str] = set()
    symbols = data.get("symbols", [])
    for item in symbols:
        if item.get("status", item.get("contractStatus")) != "TRADING":
            continue
        if market_type == MarketType.SPOT:
            if item.get("quoteAsset") == "USDT":
                bases.add(item["baseAsset"].upper())
        elif market_type == MarketType.USDT_PERP:
            if (
                item.get("contractType") == "PERPETUAL"
                and item.get("quoteAsset") == "USDT"
            ):
                bases.add(item["baseAsset"].upper())
        elif market_type == MarketType.COIN_PERP:
            if (
                item.get("contractType") == "PERPETUAL"
                and item.get("marginAsset", item.get("quoteAsset")) == "USD"
            ):
                pair = item.get("pair", item.get("symbol", ""))
                base = pair.replace("USD", "").replace("_PERP", "").strip("_")
                if base:
                    bases.add(base.upper())
    return bases


def _extract_okx_bases(data: dict[str, Any], market_type: MarketType) -> set[str]:
    bases: set[str] = set()
    instruments = data.get("data", [])
    for item in instruments:
        inst_id: str = item.get("instId", "")
        state: str = item.get("state", "")
        if state != "live":
            continue
        if market_type == MarketType.SPOT:
            if inst_id.endswith("-USDT") and "-SWAP" not in inst_id:
                bases.add(inst_id.split("-")[0].upper())
        elif market_type == MarketType.USDT_PERP:
            if inst_id.endswith("-USDT-SWAP"):
                bases.add(inst_id.split("-")[0].upper())
        elif market_type == MarketType.COIN_PERP:
            if inst_id.endswith("-USD-SWAP"):
                bases.add(inst_id.split("-")[0].upper())
    return bases


def _okx_inst_type(market_type: MarketType) -> str:
    if market_type == MarketType.SPOT:
        return "SPOT"
    return "SWAP"


async def discover_common_pairs(
    market_type: MarketType,
    *,
    session: aiohttp.ClientSession | None = None,
) -> list[str]:
    own_session = session is None
    if own_session:
        session = aiohttp.ClientSession()

    try:
        binance_url = binance_exchange_info_url(market_type)
        okx_inst_type = _okx_inst_type(market_type)

        binance_data, okx_data = await _fetch_json(session, binance_url), None
        okx_data = await _fetch_json(
            session, _OKX_INSTRUMENTS_URL, instType=okx_inst_type
        )

        binance_bases = _extract_binance_bases(binance_data, market_type)
        okx_bases = _extract_okx_bases(okx_data, market_type)
        common_bases = sorted(binance_bases & okx_bases)

        pairs = [okx_symbol(base, market_type) for base in common_bases]
        logger.info(
            "Discovered %d common %s pairs (Binance: %d, OKX: %d)",
            len(pairs),
            market_type.value,
            len(binance_bases),
            len(okx_bases),
        )
        return pairs
    finally:
        if own_session:
            await session.close()
