"""
LLM 批处理任务（Section 08）

包含：
- run_llm_review_batch: 批量 Reviewer 任务（插入点A）
- run_llm_analyst: 单 symbol Analyst 任务（插入点B）
- run_llm_pipeline: 完整 LLM 流水线
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from ..database.repository import PatternRepository
from ..exceptions import LLMCallError, LLMParseError
from ..llm.analyst import LLMAnalyst, build_analyst_input
from ..llm.base import CircuitBreakerOpen, LLMClient
from ..llm.narrator import LLMNarrator
from ..llm.reviewer import LLMReviewer, scan_result_to_reviewer_input
from ..llm.schemas import LLMNarratorInput
from ..models import PatternScanResult, RegimeResult

logger = logging.getLogger(__name__)

REVIEWER_CONCURRENCY = 5
ANALYST_CONCURRENCY  = 2
LLM_TASK_TIMEOUT     = 45.0

PROMPT_VERSION = 'v1'


@dataclass
class LLMBatchStats:
    total:        int   = 0
    success:      int   = 0
    timeout:      int   = 0
    error:        int   = 0
    skipped:      int   = 0
    cb_open:      bool  = False
    duration_sec: float = 0.0


# ── 批量 Reviewer ─────────────────────────────────────────────────────────────

async def run_llm_review_batch(
    results:     list[PatternScanResult],
    reviewer:    LLMReviewer,
    repository:  PatternRepository,
    concurrency: int = REVIEWER_CONCURRENCY,
) -> LLMBatchStats:
    """
    对候选形态列表并发执行 LLM Reviewer 审核，并将结果写回数据库。
    """
    stats = LLMBatchStats(total=len(results))
    if not results:
        return stats

    t0  = time.monotonic()
    sem = asyncio.Semaphore(concurrency)
    cb_triggered = asyncio.Event()

    async def _review_one(result: PatternScanResult) -> None:
        if result.db_id is None or cb_triggered.is_set():
            stats.skipped += 1
            return

        async with sem:
            if cb_triggered.is_set():
                stats.skipped += 1
                return
            try:
                inp = scan_result_to_reviewer_input(result)
                out = await asyncio.wait_for(
                    reviewer.review(inp, db_id=result.db_id),
                    timeout=LLM_TASK_TIMEOUT,
                )
                await repository.update_llm_review(
                    result_id      = result.db_id,
                    confidence     = out.confidence,
                    risk           = out.risk,
                    enter_pool     = out.enter_pool,
                    reasoning      = out.reasoning,
                    prompt_version = PROMPT_VERSION,
                )
                stats.success += 1

            except CircuitBreakerOpen:
                stats.cb_open = True
                cb_triggered.set()
                stats.skipped += 1

            except asyncio.TimeoutError:
                stats.timeout += 1
                logger.warning('LLM review timeout: %s/%s', result.symbol, result.pattern_id)

            except (LLMCallError, LLMParseError) as e:
                stats.error += 1
                logger.warning('LLM review error %s/%s: %s', result.symbol, result.pattern_id, e)

            except Exception as e:
                stats.error += 1
                logger.error('Unexpected review error: %s', e, exc_info=True)

    await asyncio.gather(*[_review_one(r) for r in results])

    stats.duration_sec = time.monotonic() - t0
    logger.info(
        'Review batch done: total=%d success=%d timeout=%d error=%d skipped=%d (%.1fs)',
        stats.total, stats.success, stats.timeout, stats.error, stats.skipped, stats.duration_sec,
    )
    return stats


# ── Analyst（按 symbol 分组）─────────────────────────────────────────────────

async def run_llm_analyst(
    symbol:        str,
    timeframe:     str,
    candidates:    list[PatternScanResult],
    analyst:       LLMAnalyst,
    narrator:      LLMNarrator,
    repository:    PatternRepository,
    batch_id:      str,
    regime_result: Optional[RegimeResult] = None,
    btc_context:   Optional[dict] = None,
) -> bool:
    """
    对单个 symbol 的候选形态执行深度分析（Analyst + Narrator），
    并将报告写入数据库。
    """
    if not candidates:
        return False

    try:
        inp = build_analyst_input(
            symbol        = symbol,
            timeframe     = timeframe,
            candidates    = candidates,
            regime_result = regime_result,
            btc_context   = btc_context,
        )

        analyst_out = await asyncio.wait_for(
            analyst.analyze(inp),
            timeout=LLM_TASK_TIMEOUT,
        )

        # Narrator 叙述（可选）
        btc_narrative: Optional[str] = None
        try:
            narrator_inp = LLMNarratorInput(
                symbol         = symbol,
                timeframe      = timeframe,
                analyst_report = analyst_out,
            )
            narrator_out  = await asyncio.wait_for(
                narrator.narrate(narrator_inp),
                timeout=LLM_TASK_TIMEOUT,
            )
            btc_narrative = narrator_out.narrative
        except Exception as ne:
            logger.warning('Narrator failed for %s: %s', symbol, ne)

        # 将 top candidates 按方向分组，兼容现有 DB schema
        top_long  = [
            {'symbol': c.symbol, 'pattern_id': c.pattern_id, 'score': c.total_score}
            for c in candidates if c.direction == 'long'
        ]
        top_short = [
            {'symbol': c.symbol, 'pattern_id': c.pattern_id, 'score': c.total_score}
            for c in candidates if c.direction == 'short'
        ]

        # 构建兼容 save_analyst_report 的 output 对象
        report_output = _AnalystReportCompat(
            top_long       = top_long,
            top_short      = top_short,
            warnings       = analyst_out.tags,
            market_summary = analyst_out.reasoning,
            prompt_version = PROMPT_VERSION,
        )

        await repository.save_analyst_report(
            scan_batch_id   = batch_id,
            output          = report_output,
            btc_regime      = analyst_out.direction_bias,
            btc_narrative   = btc_narrative or analyst_out.reasoning,
            candidate_count = len(candidates),
        )
        return True

    except CircuitBreakerOpen:
        logger.warning('Circuit breaker open, skipping analyst for %s', symbol)
        return False
    except asyncio.TimeoutError:
        logger.warning('Analyst timeout for %s', symbol)
        return False
    except (LLMCallError, LLMParseError) as e:
        logger.warning('Analyst error for %s: %s', symbol, e)
        return False
    except Exception as e:
        logger.error('Unexpected analyst error for %s: %s', symbol, e, exc_info=True)
        return False


class _AnalystReportCompat:
    """兼容 save_analyst_report 期望接口的轻量包装"""
    def __init__(self, top_long, top_short, warnings, market_summary, prompt_version):
        self.top_long       = top_long
        self.top_short      = top_short
        self.warnings       = warnings
        self.market_summary = market_summary
        self.prompt_version = prompt_version


# ── 完整 LLM 流水线 ──────────────────────────────────────────────────────────

async def run_llm_pipeline(
    all_results:             list[PatternScanResult],
    client:                  LLMClient,
    repository:              PatternRepository,
    batch_id:                str = '',
    run_analyst:             bool = True,
    analyst_min_score:       float = 75.0,
    analyst_require_trigger: bool = False,
) -> LLMBatchStats:
    """
    完整 LLM 处理流水线：
    1. Reviewer 批量审核所有候选
    2. 筛选 enter_pool=True 的候选
    3. 按 symbol 分组，执行 Analyst + Narrator
    """
    overall_stats = LLMBatchStats(total=len(all_results))
    if not all_results:
        return overall_stats

    t0 = time.monotonic()

    reviewer = LLMReviewer(client)
    analyst  = LLMAnalyst(client)
    narrator = LLMNarrator(client)

    # ── Step 1: Reviewer ──────────────────────────────────────────────────────
    review_stats = await run_llm_review_batch(all_results, reviewer, repository)
    overall_stats.success += review_stats.success
    overall_stats.timeout += review_stats.timeout
    overall_stats.error   += review_stats.error
    overall_stats.skipped += review_stats.skipped

    if review_stats.cb_open:
        overall_stats.cb_open = True
        overall_stats.duration_sec = time.monotonic() - t0
        return overall_stats

    if not run_analyst or not batch_id:
        overall_stats.duration_sec = time.monotonic() - t0
        return overall_stats

    # ── Step 2: 筛选进入 Analyst 的候选 ───────────────────────────────────────
    try:
        analyst_candidates_raw = await repository.get_candidates_for_analyst(
            scan_batch_id = batch_id,
            min_score     = analyst_min_score,
            limit         = 50,
        )
    except Exception as e:
        logger.error('Failed to fetch analyst candidates: %s', e)
        overall_stats.duration_sec = time.monotonic() - t0
        return overall_stats

    if not analyst_candidates_raw:
        logger.info('No candidates passed review for analyst stage')
        overall_stats.duration_sec = time.monotonic() - t0
        return overall_stats

    # 将 dicts 转回 PatternScanResult（利用已有的 result 对象）
    result_by_id = {r.db_id: r for r in all_results if r.db_id is not None}
    analyst_candidates: list[PatternScanResult] = []
    for raw in analyst_candidates_raw:
        # raw is a dict with symbol/pattern_id etc., find matching result
        matched = next(
            (r for r in all_results
             if r.symbol == raw.get('symbol') and r.pattern_id == raw.get('pattern_id')),
            None,
        )
        if matched:
            analyst_candidates.append(matched)

    # filter by enter_pool
    analyst_candidates = [
        r for r in analyst_candidates
        if r.llm_enter_pool is True
    ]

    if not analyst_candidates:
        overall_stats.duration_sec = time.monotonic() - t0
        return overall_stats

    # ── Step 3: 按 symbol 分组，并发执行 Analyst ───────────────────────────────
    by_symbol: dict[str, list[PatternScanResult]] = {}
    for r in analyst_candidates:
        key = f'{r.symbol}|{r.timeframe}'
        by_symbol.setdefault(key, []).append(r)

    sem = asyncio.Semaphore(ANALYST_CONCURRENCY)

    async def _analyst_one(key: str, cands: list[PatternScanResult]) -> None:
        symbol, tf = key.split('|', 1)
        async with sem:
            await run_llm_analyst(
                symbol     = symbol,
                timeframe  = tf,
                candidates = cands,
                analyst    = analyst,
                narrator   = narrator,
                repository = repository,
                batch_id   = batch_id,
            )

    await asyncio.gather(*[
        _analyst_one(key, cands)
        for key, cands in by_symbol.items()
    ])

    overall_stats.duration_sec = time.monotonic() - t0
    logger.info(
        'LLM pipeline done: %d candidates → %d analyst groups (%.1fs)',
        len(analyst_candidates), len(by_symbol), overall_stats.duration_sec,
    )
    return overall_stats
