"""
BinanceFetcher — Binance USDT永续合约K线数据拉取（Section 06）

特性：
- aiohttp Session 复用（单例 session，跨请求共享连接池）
- 指数退避重试（最多3次）
- 速率限制：并发10，间隔50ms
- 标准化输出：open_time, open, high, low, close, volume
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

import aiohttp
import pandas as pd

from ..exceptions import FetchError, InsufficientDataError
from .base import MarketDataProvider

logger = logging.getLogger(__name__)

BINANCE_BASE_URL   = 'https://fapi.binance.com'
KLINE_ENDPOINT     = '/fapi/v1/klines'
EXCHANGE_ENDPOINT  = '/fapi/v1/exchangeInfo'

MAX_RETRIES        = 3
RETRY_BASE_DELAY   = 1.0   # seconds
SEMAPHORE_LIMIT    = 10    # max concurrent requests
REQUEST_INTERVAL   = 0.05  # 50ms between requests

KLINE_COLUMNS = [
    'open_time', 'open', 'high', 'low', 'close', 'volume',
    'close_time', 'quote_volume', 'trades_count',
    'taker_buy_base', 'taker_buy_quote', 'ignore',
]
NUMERIC_COLS = ['open', 'high', 'low', 'close', 'volume']


class BinanceFetcher(MarketDataProvider):
    """
    Binance USDT永续合约数据拉取器。
    应在整个批次生命周期内复用同一实例以共享连接池。
    """

    def __init__(
        self,
        base_url: str = BINANCE_BASE_URL,
        timeout_sec: float = 10.0,
        proxy: Optional[str] = None,
    ):
        self._base_url   = base_url
        self._timeout    = aiohttp.ClientTimeout(total=timeout_sec)
        self._proxy      = proxy
        self._session:   Optional[aiohttp.ClientSession] = None
        self._semaphore  = asyncio.Semaphore(SEMAPHORE_LIMIT)
        self._last_request_time: float = 0.0

    # ── 会话管理 ───────────────────────────────────────────────────────────────

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            connector = aiohttp.TCPConnector(limit=SEMAPHORE_LIMIT, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
                headers={'User-Agent': 'PatternScanner/1.0'},
            )
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ── 速率控制 ───────────────────────────────────────────────────────────────

    async def _rate_limit(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_request_time
        if elapsed < REQUEST_INTERVAL:
            await asyncio.sleep(REQUEST_INTERVAL - elapsed)
        self._last_request_time = time.monotonic()

    # ── K线拉取 ────────────────────────────────────────────────────────────────

    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        end_time: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        拉取K线，最多 limit 根，从 end_time 往前。
        返回按 open_time 升序排列的 DataFrame。
        """
        params: dict = {'symbol': symbol, 'interval': interval, 'limit': limit}
        if end_time is not None:
            params['endTime'] = end_time

        raw = await self._request_with_retry(KLINE_ENDPOINT, params)

        if not raw:
            raise InsufficientDataError(f'{symbol}/{interval}: empty response')

        df = pd.DataFrame(raw, columns=KLINE_COLUMNS)
        df['open_time'] = df['open_time'].astype('int64')
        for col in NUMERIC_COLS:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']].copy()
        df.set_index('open_time', inplace=True)
        df.sort_index(inplace=True)
        df.dropna(inplace=True)

        return df

    async def fetch_klines_full(
        self,
        symbol: str,
        interval: str,
        total_bars: int = 1000,
    ) -> pd.DataFrame:
        """
        分页拉取，支持超过500根K线的请求。
        """
        all_dfs: list[pd.DataFrame] = []
        end_time: Optional[int] = None
        remaining = total_bars

        while remaining > 0:
            limit  = min(remaining, 500)
            df     = await self.fetch_klines(symbol, interval, limit=limit, end_time=end_time)

            if df.empty:
                break

            all_dfs.append(df)
            remaining -= len(df)

            if len(df) < limit:
                break

            # 下一页的 end_time = 当前页最早的 open_time - 1ms
            end_time = int(df.index[0]) - 1

        if not all_dfs:
            raise InsufficientDataError(f'{symbol}/{interval}: no data fetched')

        combined = pd.concat(all_dfs).sort_index()
        combined = combined[~combined.index.duplicated(keep='last')]
        return combined

    # ── 交易对列表 ─────────────────────────────────────────────────────────────

    async def get_usdt_perpetual_symbols(self) -> list[str]:
        """返回所有 USDT 永续合约交易对，过滤掉已下架的"""
        data = await self._request_with_retry(EXCHANGE_ENDPOINT, {})
        symbols = []
        for s in data.get('symbols', []):
            if (
                s.get('quoteAsset') == 'USDT'
                and s.get('contractType') == 'PERPETUAL'
                and s.get('status') == 'TRADING'
            ):
                symbols.append(s['symbol'])
        return sorted(symbols)

    # ── 底层请求 ───────────────────────────────────────────────────────────────

    async def _request_with_retry(self, endpoint: str, params: dict):
        """带指数退避重试的 GET 请求"""
        url = f'{self._base_url}{endpoint}'
        last_error: Optional[Exception] = None

        for attempt in range(MAX_RETRIES):
            try:
                async with self._semaphore:
                    await self._rate_limit()
                    session = await self._get_session()
                    async with session.get(
                        url, params=params, proxy=self._proxy
                    ) as resp:
                        if resp.status == 429:
                            retry_after = float(resp.headers.get('Retry-After', 5))
                            logger.warning('Rate limited, sleeping %.1fs', retry_after)
                            await asyncio.sleep(retry_after)
                            continue

                        if resp.status != 200:
                            text = await resp.text()
                            raise FetchError(
                                f'HTTP {resp.status} for {endpoint}: {text[:200]}'
                            )

                        return await resp.json()

            except FetchError:
                raise
            except asyncio.TimeoutError as e:
                last_error = FetchError(f'Timeout on {endpoint} (attempt {attempt + 1})')
                logger.warning('%s', last_error)
            except aiohttp.ClientError as e:
                last_error = FetchError(f'Client error on {endpoint}: {e}')
                logger.warning('%s', last_error)
            except Exception as e:
                last_error = FetchError(f'Unexpected error on {endpoint}: {e}')
                logger.warning('%s', last_error)

            if attempt < MAX_RETRIES - 1:
                delay = RETRY_BASE_DELAY * (2 ** attempt)
                await asyncio.sleep(delay)

        raise last_error or FetchError(f'Failed after {MAX_RETRIES} retries: {endpoint}')
