"""
PatternRepository 数据库层测试（内存SQLite）
"""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest
import pytest_asyncio

from ..database.repository import PatternRepository
from ..database.session import init_db, create_tables, dispose_engine
from ..models import PatternScanResult


@pytest_asyncio.fixture(autouse=True)
async def setup_db():
    init_db('sqlite+aiosqlite:///:memory:')
    await create_tables()
    yield
    await dispose_engine()


@pytest.fixture
def repo():
    return PatternRepository()


def _make_result(**kwargs) -> PatternScanResult:
    defaults = dict(
        symbol          = 'TESTUSDT',
        timeframe       = '4h',
        bar_time        = datetime(2024, 1, 1, 0, 0, 0),
        pattern_id      = 'A1',
        pattern_name    = 'Test Pattern',
        direction       = 'long',
        regime          = 'bull_trend',
        regime_score    = 80.0,
        total_score     = 75.0,
        confirm_score   = 6.0,
        exclude_penalty = 0.0,
        field_results   = {'f1': True, 'f2': False},
        raw_values      = {'f1': 1.5, 'f2': 0.3},
        trigger_met     = True,
        trigger_type    = 'breakout',
        rule_version    = '1.0',
    )
    defaults.update(kwargs)
    return PatternScanResult(**defaults)


def _make_df(n=5) -> pd.DataFrame:
    """生成测试用 OHLCV DataFrame"""
    import numpy as np
    ts = pd.date_range('2024-01-01', periods=n, freq='4h')
    rng = np.random.default_rng(42)
    close = 40000 + rng.normal(0, 100, n)
    return pd.DataFrame({
        'open':   close * 0.999,
        'high':   close * 1.005,
        'low':    close * 0.995,
        'close':  close,
        'volume': rng.uniform(1000, 5000, n),
    }, index=ts)


@pytest.mark.asyncio
async def test_bulk_save_returns_ids(repo):
    results = [_make_result(), _make_result(symbol='ETHUSDT', total_score=85.0)]
    db_ids = await repo.bulk_save(results)
    assert len(db_ids) == 2
    assert all(isinstance(i, int) for i in db_ids)


@pytest.mark.asyncio
async def test_get_by_id(repo):
    db_ids = await repo.bulk_save([_make_result()])
    fetched = await repo.get_by_id(db_ids[0])
    assert fetched is not None
    assert fetched.symbol == 'TESTUSDT'
    assert fetched.total_score == pytest.approx(75.0)


@pytest.mark.asyncio
async def test_update_llm_review(repo):
    db_ids = await repo.bulk_save([_make_result()])

    await repo.update_llm_review(
        result_id      = db_ids[0],
        confidence     = 'high',
        enter_pool     = True,
        risk           = 'low',
        reasoning      = 'Strong signal',
        prompt_version = 'v1',
    )

    fetched = await repo.get_by_id(db_ids[0])
    assert fetched.llm_confidence == 'high'
    assert fetched.llm_enter_pool is True


@pytest.mark.asyncio
async def test_upsert_symbols(repo):
    rows = [
        {'symbol': 'BTCUSDT', 'is_active': True, 'updated_at': datetime.utcnow()},
        {'symbol': 'ETHUSDT', 'is_active': True, 'updated_at': datetime.utcnow()},
    ]
    await repo.upsert_symbols(rows)
    symbols = await repo.get_active_symbols()
    assert 'BTCUSDT' in symbols
    assert 'ETHUSDT' in symbols


@pytest.mark.asyncio
async def test_upsert_klines(repo):
    """测试K线 upsert：传入 DataFrame"""
    df = _make_df(5)
    count = await repo.upsert_klines('BTCUSDT', '4h', df)
    assert count == 5

    result_df = await repo.get_klines('BTCUSDT', '4h', limit=10)
    assert isinstance(result_df, pd.DataFrame)
    assert len(result_df) == 5


@pytest.mark.asyncio
async def test_upsert_klines_idempotent(repo):
    """重复插入不应增加记录数"""
    df = _make_df(5)
    await repo.upsert_klines('BTCUSDT', '4h', df)
    await repo.upsert_klines('BTCUSDT', '4h', df)  # 重复
    result_df = await repo.get_klines('BTCUSDT', '4h', limit=20)
    assert len(result_df) == 5


@pytest.mark.asyncio
async def test_start_finish_job_log(repo):
    batch_id = 'test-batch-001'
    await repo.start_job_log(
        job_id   = batch_id,
        batch_id = batch_id,
        interval = '4h',
    )
    await repo.finish_job_log(batch_id, status='success')
    # 完成的任务不应在 stale 列表中
    stale = await repo.get_stale_jobs(stale_minutes=60)
    stale_ids = [s.job_id for s in stale]
    assert batch_id not in stale_ids


@pytest.mark.asyncio
async def test_get_candidates_for_analyst_empty(repo):
    """没有候选时返回空列表"""
    candidates = await repo.get_candidates_for_analyst(
        scan_batch_id = 'nonexistent-batch',
        min_score     = 70.0,
    )
    assert candidates == []


@pytest.mark.asyncio
async def test_get_scan_history_empty(repo):
    history = await repo.get_scan_history()
    assert isinstance(history, list)
    assert history == []


@pytest.mark.asyncio
async def test_get_scan_history_with_data(repo):
    results = [
        _make_result(pattern_id='A1', scan_batch_id='batch1'),
        _make_result(symbol='ETHUSDT', pattern_id='B1',
                     direction='short', scan_batch_id='batch1'),
    ]
    await repo.bulk_save(results)
    history = await repo.get_scan_history()
    assert len(history) == 2
    pattern_ids = {r['pattern_id'] for r in history}
    assert 'A1' in pattern_ids
    assert 'B1' in pattern_ids
