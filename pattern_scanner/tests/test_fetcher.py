"""
BinanceFetcher 测试（使用 aioresponses mock）
"""
from __future__ import annotations

import json
import pytest
import pytest_asyncio

from ..data.fetcher import BinanceFetcher
from ..exceptions import FetchError, InsufficientDataError


def _make_kline_row(open_time=1000000, close=100.0):
    return [
        open_time, str(close - 0.5), str(close + 1),
        str(close - 1), str(close), '1000.0',
        open_time + 3599999, '100000.0', 100,
        '500.0', '50000.0', '0',
    ]


@pytest.mark.asyncio
async def test_fetch_klines_parses_response():
    """测试正常响应的解析"""
    fetcher = BinanceFetcher()
    try:
        # 生成合成数据（避免真实网络请求）
        rows = [_make_kline_row(1000000 + i * 14400000, 100.0 + i) for i in range(10)]

        import pandas as pd
        import numpy as np
        from unittest.mock import AsyncMock, patch, MagicMock

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=rows)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        with patch.object(fetcher, '_get_session', AsyncMock(return_value=mock_session)):
            df = await fetcher.fetch_klines('BTCUSDT', '4h', limit=10)

        assert len(df) == 10
        assert 'close' in df.columns
        assert 'volume' in df.columns
        assert df['close'].iloc[-1] == pytest.approx(109.0)

    finally:
        await fetcher.close()


@pytest.mark.asyncio
async def test_fetch_klines_empty_raises():
    """空响应应抛出 InsufficientDataError"""
    fetcher = BinanceFetcher()
    try:
        from unittest.mock import AsyncMock, patch, MagicMock

        mock_resp = MagicMock()
        mock_resp.status = 200
        mock_resp.json = AsyncMock(return_value=[])
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        with patch.object(fetcher, '_get_session', AsyncMock(return_value=mock_session)):
            with pytest.raises(InsufficientDataError):
                await fetcher.fetch_klines('BTCUSDT', '4h', limit=10)

    finally:
        await fetcher.close()


@pytest.mark.asyncio
async def test_fetch_http_error_raises():
    """HTTP 错误应抛出 FetchError"""
    fetcher = BinanceFetcher()
    try:
        from unittest.mock import AsyncMock, patch, MagicMock

        mock_resp = MagicMock()
        mock_resp.status = 500
        mock_resp.text = AsyncMock(return_value='Internal Server Error')
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_session.closed = False

        with patch.object(fetcher, '_get_session', AsyncMock(return_value=mock_session)):
            with pytest.raises(FetchError):
                await fetcher.fetch_klines('BTCUSDT', '4h', limit=10)

    finally:
        await fetcher.close()
