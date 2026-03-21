"""
SymbolUniverse — 交易对全集管理（Section 06）

负责：
- 从 Binance 获取并缓存 USDT 永续合约列表
- 过滤黑名单、最小成交量过滤
- 与数据库 SymbolUniverseORM 同步（upsert）
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Optional

from ..database.repository import PatternRepository
from .fetcher import BinanceFetcher

logger = logging.getLogger(__name__)

# 默认黑名单（稳定币、杠杆代币等）
DEFAULT_BLACKLIST: set[str] = {
    'USDCUSDT', 'BUSDUSDT', 'TUSDUSDT', 'USDTUSDT',
    'BTCDOMUSDT',  # 比特币市值占比，非标准合约
}

# 24h成交量过滤下限（USDT）
MIN_VOLUME_USDT = 5_000_000


class SymbolUniverse:
    """
    交易对全集管理器。
    通常每个扫描批次开始时刷新一次，结果缓存在内存中。
    """

    def __init__(
        self,
        fetcher: BinanceFetcher,
        repository: Optional[PatternRepository] = None,
        blacklist: Optional[set[str]] = None,
        cache_ttl_minutes: int = 60,
    ):
        self._fetcher      = fetcher
        self._repo         = repository
        self._blacklist    = blacklist or DEFAULT_BLACKLIST
        self._cache_ttl    = timedelta(minutes=cache_ttl_minutes)
        self._symbols:     list[str] = []
        self._fetched_at:  Optional[datetime] = None

    # ──────────────────────────────────────────────────────────────────────────

    async def get_symbols(self, force_refresh: bool = False) -> list[str]:
        """
        返回当前可用的 USDT 永续合约列表。
        在 TTL 内使用缓存，超出或 force_refresh 时重新拉取。
        """
        if not force_refresh and self._is_cache_valid():
            return self._symbols

        await self._refresh()
        return self._symbols

    async def _refresh(self) -> None:
        try:
            raw = await self._fetcher.get_usdt_perpetual_symbols()
            filtered = [s for s in raw if s not in self._blacklist]
            self._symbols   = filtered
            self._fetched_at = datetime.utcnow()
            logger.info('SymbolUniverse refreshed: %d symbols', len(filtered))

            if self._repo:
                await self._sync_to_db(filtered)

        except Exception as e:
            logger.error('SymbolUniverse refresh failed: %s', e)
            # 保持旧缓存，不清空
            if not self._symbols:
                raise

    async def _sync_to_db(self, symbols: list[str]) -> None:
        """将当前活跃交易对同步到数据库"""
        try:
            rows = [
                {
                    'symbol':      s,
                    'is_active':   True,
                    'updated_at':  datetime.utcnow(),
                }
                for s in symbols
            ]
            await self._repo.upsert_symbols(rows)
        except Exception as e:
            logger.warning('SymbolUniverse DB sync failed: %s', e)

    def _is_cache_valid(self) -> bool:
        if not self._symbols or self._fetched_at is None:
            return False
        return datetime.utcnow() - self._fetched_at < self._cache_ttl

    # ──────────────────────────────────────────────────────────────────────────

    async def get_scannable_symbols(
        self,
        repository: Optional[PatternRepository] = None,
        force_refresh: bool = False,
        timeframe: str = '4h',
    ) -> list[str]:
        """
        返回数据充足的可扫描交易对（基于数据库缓存健康度）。
        若 repository 未提供，则返回所有已过滤交易对。
        """
        symbols = await self.get_symbols(force_refresh=force_refresh)
        repo = repository or self._repo

        if repo is None:
            return symbols

        try:
            unhealthy = await repo.get_unhealthy_symbols(interval=timeframe, min_bars=200)
            unhealthy_set = set(unhealthy)
            scannable = [s for s in symbols if s not in unhealthy_set]
            logger.info(
                'Scannable: %d/%d (excluded %d unhealthy)',
                len(scannable), len(symbols), len(unhealthy_set),
            )
            return scannable
        except Exception as e:
            logger.warning('Health check failed, using all symbols: %s', e)
            return symbols

    def symbol_count(self) -> int:
        return len(self._symbols)

    def is_known(self, symbol: str) -> bool:
        return symbol in self._symbols
