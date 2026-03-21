"""
pytest 共享 fixtures
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import pytest_asyncio

from ..database.session import init_db, dispose_engine, get_session
from ..indicators import IndicatorLibrary
from ..patterns.definitions import ALL_PATTERNS
from ..scanner import PatternScanner


# ── DataFrame fixtures ────────────────────────────────────────────────────────

def _make_df(
    n: int = 200,
    trend: str = 'up',      # 'up', 'down', 'flat'
    seed: int = 42,
) -> pd.DataFrame:
    """生成带趋势的合成 OHLCV DataFrame"""
    rng = np.random.default_rng(seed)
    ts  = pd.date_range('2024-01-01', periods=n, freq='4h')

    if trend == 'up':
        close = 100 + np.cumsum(rng.normal(0.2, 1.0, n))
    elif trend == 'down':
        close = 200 - np.cumsum(rng.normal(0.2, 1.0, n))
    else:
        close = 100 + rng.normal(0, 0.5, n)

    close = np.maximum(close, 1.0)
    high  = close * (1 + rng.uniform(0.0, 0.03, n))
    low   = close * (1 - rng.uniform(0.0, 0.03, n))
    open_ = close * (1 + rng.normal(0, 0.005, n))
    vol   = rng.uniform(1e6, 5e6, n)

    df = pd.DataFrame({
        'open':   open_,
        'high':   high,
        'low':    low,
        'close':  close,
        'volume': vol,
    }, index=ts)
    return df


@pytest.fixture
def df_up():
    return _make_df(200, trend='up')


@pytest.fixture
def df_down():
    return _make_df(200, trend='down')


@pytest.fixture
def df_flat():
    return _make_df(200, trend='flat')


@pytest.fixture
def df_short():
    """不足30根K线的短序列"""
    return _make_df(20, trend='up')


# ── IndicatorLibrary fixture ──────────────────────────────────────────────────

@pytest.fixture
def indicators(df_up):
    return IndicatorLibrary()


# ── Scanner fixture ───────────────────────────────────────────────────────────

@pytest.fixture
def scanner():
    return PatternScanner(patterns=ALL_PATTERNS)


# ── Database fixtures（SQLite内存库）─────────────────────────────────────────

@pytest_asyncio.fixture
async def db_session():
    """每个测试使用独立的内存SQLite数据库"""
    init_db('sqlite+aiosqlite:///:memory:')
    from ..database.session import create_tables
    await create_tables()
    yield
    await dispose_engine()
