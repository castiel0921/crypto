"""
MarketDataProvider — 数据提供者抽象基类（Section 06）
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class MarketDataProvider(ABC):
    """
    所有市场数据源的抽象接口。
    实现类：BinanceFetcher（实时）、LocalCSVProvider（测试）
    """

    @abstractmethod
    async def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        end_time: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        获取K线数据，返回标准化 DataFrame。

        列名规范：
            open_time (ms int), open, high, low, close, volume

        Raises:
            FetchError: 网络或解析错误
            InsufficientDataError: 返回数据不足
        """

    @abstractmethod
    async def get_usdt_perpetual_symbols(self) -> list[str]:
        """返回所有 USDT 永续合约交易对列表"""

    @abstractmethod
    async def close(self) -> None:
        """释放底层连接资源"""
