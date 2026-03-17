from __future__ import annotations

from enum import Enum


class MarketType(str, Enum):
    SPOT = "spot"
    USDT_PERP = "usdt_perp"
    COIN_PERP = "coin_perp"


# Binance WebSocket base URLs per market type
_BINANCE_WS_URLS: dict[MarketType, str] = {
    MarketType.SPOT: "wss://data-stream.binance.vision",
    MarketType.USDT_PERP: "wss://fstream.binance.com",
    MarketType.COIN_PERP: "wss://dstream.binance.com",
}

# Binance REST base URLs per market type
_BINANCE_REST_URLS: dict[MarketType, str] = {
    MarketType.SPOT: "https://api.binance.com",
    MarketType.USDT_PERP: "https://fapi.binance.com",
    MarketType.COIN_PERP: "https://dapi.binance.com",
}

# Binance exchangeInfo paths per market type
_BINANCE_EXCHANGE_INFO_PATHS: dict[MarketType, str] = {
    MarketType.SPOT: "/api/v3/exchangeInfo",
    MarketType.USDT_PERP: "/fapi/v1/exchangeInfo",
    MarketType.COIN_PERP: "/dapi/v1/exchangeInfo",
}

MARKET_TYPE_LABELS: dict[MarketType, str] = {
    MarketType.SPOT: "现货",
    MarketType.USDT_PERP: "U本位永续",
    MarketType.COIN_PERP: "币本位永续",
}


def okx_symbol(base: str, market_type: MarketType) -> str:
    if market_type == MarketType.SPOT:
        return f"{base}-USDT"
    elif market_type == MarketType.USDT_PERP:
        return f"{base}-USDT-SWAP"
    else:
        return f"{base}-USD-SWAP"


def binance_symbol(base: str, market_type: MarketType) -> str:
    if market_type in (MarketType.SPOT, MarketType.USDT_PERP):
        return f"{base}USDT"
    else:
        return f"{base}USD_PERP"


def binance_ws_base_url(market_type: MarketType) -> str:
    return _BINANCE_WS_URLS[market_type]


def binance_rest_base_url(market_type: MarketType) -> str:
    return _BINANCE_REST_URLS[market_type]


def binance_exchange_info_url(market_type: MarketType) -> str:
    return _BINANCE_REST_URLS[market_type] + _BINANCE_EXCHANGE_INFO_PATHS[market_type]


def binance_stream_name(base: str, market_type: MarketType) -> str:
    sym = binance_symbol(base, market_type).lower()
    return f"{sym}@bookTicker"


def canonical_symbol(base: str, market_type: MarketType) -> str:
    return okx_symbol(base, market_type)


def base_from_okx_symbol(okx_sym: str) -> str:
    return okx_sym.split("-")[0]
