"""
Pattern Scanner 主流水线（Section 10）

run_full_pipeline:  单次完整扫描批次（数据获取 + 形态识别 + LLM审核）
run_batch_pipeline: 仅数据获取 + 形态识别（无LLM）
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from datetime import datetime
from typing import Optional

import pandas as pd

from .config import build_config
from .data.fetcher import BinanceFetcher
from .data.universe import SymbolUniverse
from .database.repository import PatternRepository
from .database.session import init_db
from .exceptions import FetchError, InsufficientDataError
from .llm.base import LLMClient
from .models import JobSummary, PatternScanResult
from .patterns.definitions import ALL_PATTERNS
from .scanner import PatternScanner
from .tasks.llm_tasks import run_llm_pipeline

logger = logging.getLogger(__name__)

DEFAULT_TIMEFRAME  = '4h'
DEFAULT_KLINE_BARS = 500


async def run_full_pipeline(
    db_url:       str,
    api_key:      str,
    timeframe:    str   = DEFAULT_TIMEFRAME,
    kline_bars:   int   = DEFAULT_KLINE_BARS,
    run_llm:      bool  = True,
    config_overrides: Optional[dict] = None,
    llm_base_url: str   = 'https://api.deepseek.com',
    llm_model:    str   = 'deepseek-chat',
) -> JobSummary:
    """
    完整扫描批次：
    1. 获取 BTC 基准数据
    2. 并发拉取全市场K线
    3. 形态识别扫描
    4. LLM Reviewer 审核（可选）
    5. LLM Analyst 深度分析（可选）
    """
    batch_id = str(uuid.uuid4())
    t_start  = time.monotonic()

    build_config(config_overrides)
    scanner = PatternScanner(patterns=ALL_PATTERNS)

    init_db(db_url)
    repo    = PatternRepository()
    fetcher = BinanceFetcher()

    await repo.start_job_log(
        job_id   = batch_id,
        batch_id = batch_id,
        interval = timeframe,
    )

    summary = JobSummary(
        batch_id               = batch_id,
        symbols_total          = 0,
        symbols_fetched        = 0,
        symbols_fetch_failed   = 0,
        symbols_scannable      = 0,
        symbols_skipped_bars   = 0,
        symbols_high_vol_skip  = 0,
        symbols_filter_hit     = 0,
        patterns_found         = 0,
        patterns_high_quality  = 0,
        patterns_trigger_met   = 0,
        llm_reviewed_count     = 0,
        llm_success_count      = 0,
        llm_timeout_count      = 0,
        llm_circuit_broken     = False,
        duration_fetch_sec     = 0.0,
        duration_scan_sec      = 0.0,
        duration_llm_sec       = 0.0,
        duration_total_sec     = 0.0,
    )

    try:
        await repo.update_job_stage(batch_id, 'fetch')

        # ── 1. BTC 基准数据 ────────────────────────────────────────────────────
        btc_df: Optional[pd.DataFrame] = None
        try:
            btc_df = await fetcher.fetch_klines('BTCUSDT', timeframe, limit=kline_bars)
            scanner.set_btc_df(btc_df)
        except Exception as e:
            logger.warning('BTC fetch failed, continuing without BTC context: %s', e)

        # ── 2. 获取交易对列表 ──────────────────────────────────────────────────
        universe = SymbolUniverse(fetcher, repository=repo)
        symbols  = await universe.get_symbols()
        summary.symbols_total = len(symbols)

        # ── 3. 并发拉取K线 ────────────────────────────────────────────────────
        t_fetch = time.monotonic()
        kline_map: dict[str, pd.DataFrame] = {}
        failed: list[str] = []

        fetch_sem = asyncio.Semaphore(10)

        async def _fetch_one(sym: str) -> None:
            async with fetch_sem:
                try:
                    df = await fetcher.fetch_klines(sym, timeframe, limit=kline_bars)
                    if len(df) >= 150:
                        kline_map[sym] = df
                    else:
                        summary.symbols_skipped_bars += 1
                except (FetchError, InsufficientDataError) as e:
                    logger.debug('Fetch failed %s: %s', sym, e)
                    failed.append(sym)
                except Exception as e:
                    logger.warning('Unexpected fetch error %s: %s', sym, e)
                    failed.append(sym)

        await asyncio.gather(*[_fetch_one(s) for s in symbols])

        summary.symbols_fetched      = len(kline_map)
        summary.symbols_fetch_failed = len(failed)
        summary.duration_fetch_sec   = time.monotonic() - t_fetch

        # 保存到数据库 K线缓存（每个 symbol 单独调用）
        for sym, df in kline_map.items():
            try:
                await repo.upsert_klines(sym, timeframe, df)
            except Exception as e:
                logger.warning('Kline upsert failed %s: %s', sym, e)

        # ── 4. 形态识别扫描 ────────────────────────────────────────────────────
        await repo.update_job_stage(batch_id, 'scan')
        t_scan = time.monotonic()

        all_scan_results: list[PatternScanResult] = []

        for sym, df in kline_map.items():
            try:
                results = await scanner.scan_latest(df, sym, timeframe)
                for r in results:
                    r.scan_batch_id = batch_id

                filter_hits  = [r for r in results if r.is_filter_hit]
                pattern_hits = [r for r in results if not r.is_filter_hit]

                if filter_hits:
                    summary.symbols_filter_hit += 1

                all_scan_results.extend(results)
                summary.patterns_found       += len(pattern_hits)
                summary.patterns_trigger_met += sum(1 for r in pattern_hits if r.trigger_met)
                summary.patterns_high_quality += sum(
                    1 for r in pattern_hits if r.total_score >= 85.0
                )

            except Exception as e:
                logger.debug('Scan error %s: %s', sym, e)

        summary.symbols_scannable = len(kline_map)
        summary.duration_scan_sec = time.monotonic() - t_scan

        # 批量保存扫描结果，获取 db_id 列表
        db_ids = await repo.bulk_save(all_scan_results)
        # 将 db_id 写回 result 对象
        for result, db_id in zip(all_scan_results, db_ids):
            result.db_id = db_id

        # ── 5. LLM 流水线 ─────────────────────────────────────────────────────
        if run_llm and api_key:
            await repo.update_job_stage(batch_id, 'llm')
            t_llm = time.monotonic()

            client = LLMClient(
                api_key  = api_key,
                base_url = llm_base_url,
                model    = llm_model,
            )

            # 只审核非过滤形态且有 db_id 的结果
            reviewable = [
                r for r in all_scan_results
                if not r.is_filter_hit and r.db_id is not None
            ]

            llm_stats = await run_llm_pipeline(
                all_results = reviewable,
                client      = client,
                repository  = repo,
                batch_id    = batch_id,
            )

            summary.llm_reviewed_count = llm_stats.total
            summary.llm_success_count  = llm_stats.success
            summary.llm_timeout_count  = llm_stats.timeout
            summary.llm_circuit_broken = llm_stats.cb_open
            summary.duration_llm_sec   = time.monotonic() - t_llm

        # ── 完成 ──────────────────────────────────────────────────────────────
        summary.duration_total_sec = time.monotonic() - t_start
        await repo.finish_job_log(
            job_id = batch_id,
            status = 'success',
            stats  = {
                'symbols_total':     summary.symbols_total,
                'symbols_fetched':   summary.symbols_fetched,
                'symbols_scannable': summary.symbols_scannable,
                'patterns_found':    summary.patterns_found,
                'llm_reviewed_count': summary.llm_reviewed_count,
                'llm_success_count':  summary.llm_success_count,
                'llm_timeout_count':  summary.llm_timeout_count,
                'duration_total_sec': summary.duration_total_sec,
            },
        )

        logger.info(
            'Pipeline done [%s]: %d symbols, %d patterns, %.1fs total',
            batch_id[:8], summary.symbols_fetched, summary.patterns_found,
            summary.duration_total_sec,
        )

    except Exception as e:
        logger.error('Pipeline failed [%s]: %s', batch_id[:8], e, exc_info=True)
        summary.duration_total_sec = time.monotonic() - t_start
        await repo.finish_job_log(batch_id, status='failed', error_message=str(e))
        raise

    finally:
        await fetcher.close()

    return summary


async def run_batch_pipeline(
    db_url:    str,
    timeframe: str = DEFAULT_TIMEFRAME,
    kline_bars: int = DEFAULT_KLINE_BARS,
    symbols:   Optional[list[str]] = None,
) -> list[PatternScanResult]:
    """
    轻量批次：仅数据获取 + 形态识别，不运行 LLM。
    适合频繁调用或测试场景。
    """
    init_db(db_url)
    fetcher = BinanceFetcher()
    scanner = PatternScanner()

    try:
        if symbols is None:
            universe = SymbolUniverse(fetcher)
            symbols = await universe.get_symbols()

        try:
            btc_df = await fetcher.fetch_klines('BTCUSDT', timeframe, limit=kline_bars)
            scanner.set_btc_df(btc_df)
        except Exception:
            pass

        all_results: list[PatternScanResult] = []
        fetch_sem = asyncio.Semaphore(10)

        async def _process(sym: str) -> None:
            async with fetch_sem:
                try:
                    df = await fetcher.fetch_klines(sym, timeframe, limit=kline_bars)
                    if len(df) < 150:
                        return
                    results = await scanner.scan_latest(df, sym, timeframe)
                    all_results.extend(results)
                except Exception as e:
                    logger.debug('Error processing %s: %s', sym, e)

        await asyncio.gather(*[_process(s) for s in symbols])
        return all_results

    finally:
        await fetcher.close()
