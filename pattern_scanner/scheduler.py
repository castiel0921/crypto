"""
APScheduler 调度器（Section 11）

任务调度：
- 每4小时运行一次完整形态扫描
- 每天凌晨2点（UTC）运行回测统计更新
- 清理旧K线缓存（保留最新 KLINE_KEEP 条）

任务状态机：
  pending → running → success/failed
  超时（>STALE_MINUTES）的 running 任务自动标记为 stale
"""
from __future__ import annotations

import asyncio
import logging
import os
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .database.repository import PatternRepository
from .database.session import init_db, dispose_engine
from .main import run_full_pipeline
from .web.server import start_web_server

logger = logging.getLogger(__name__)

ENV_DB_URL        = 'PATTERN_SCANNER_DB_URL'
ENV_API_KEY       = 'DEEPSEEK_API_KEY'
ENV_LLM_BASE_URL  = 'LLM_BASE_URL'
ENV_LLM_MODEL     = 'LLM_MODEL'
ENV_TIMEFRAMES    = 'PATTERN_SCANNER_TIMEFRAMES'   # comma-separated, e.g. "4h,1h,15m"
ENV_KLINE_BARS    = 'PATTERN_SCANNER_KLINE_BARS'
ENV_RUN_LLM       = 'PATTERN_SCANNER_RUN_LLM'

DEFAULT_DB_URL    = 'sqlite+aiosqlite:///./pattern_scanner.db'
DEFAULT_TFS       = ['4h', '1h', '15m']
DEFAULT_LLM_URL   = 'https://api.deepseek.com'
DEFAULT_LLM_MODEL = 'deepseek-chat'
STALE_MINUTES     = 35

# 每个周期对应的 kline 抓取条数和调度规则
TF_CONFIGS: dict[str, dict] = {
    '4h':  {'kline_bars': 500,  'cron': {'minute': 5},               'stale_min': 35},
    '1h':  {'kline_bars': 500,  'cron': {'minute': 6},               'stale_min': 35},
    '15m': {'kline_bars': 1000, 'cron': {'minute': '3,18,33,48'},    'stale_min': 10},
}


class PatternScannerScheduler:
    """
    Pattern Scanner 调度器。
    封装 APScheduler，管理所有周期任务。
    """

    def __init__(
        self,
        db_url:       Optional[str]        = None,
        api_key:      Optional[str]        = None,
        timeframes:   Optional[list[str]]  = None,
        run_llm:      bool                 = True,
        llm_model:    str                  = '',
        llm_base_url: str                  = '',
        web_host:     str                  = '0.0.0.0',
        web_port:     int                  = 8082,
    ):
        self._db_url      = db_url      or os.environ.get(ENV_DB_URL, DEFAULT_DB_URL)
        self._api_key     = api_key     or os.environ.get(ENV_API_KEY, '')
        self._run_llm     = run_llm
        self._llm_model   = llm_model   or os.environ.get(ENV_LLM_MODEL, DEFAULT_LLM_MODEL)
        self._llm_base_url = llm_base_url or os.environ.get(ENV_LLM_BASE_URL, DEFAULT_LLM_URL)
        self._web_host    = web_host
        self._web_port    = web_port
        self._scheduler   = AsyncIOScheduler(timezone='UTC')
        self._running     = False

        # 支持的周期列表（从参数、环境变量或默认值取）
        env_tfs = os.environ.get(ENV_TIMEFRAMES, '')
        if timeframes:
            self._timeframes = timeframes
        elif env_tfs:
            self._timeframes = [t.strip() for t in env_tfs.split(',') if t.strip()]
        else:
            self._timeframes = DEFAULT_TFS

    def setup(self) -> None:
        """注册所有定时任务（每个周期独立 job）"""
        for tf in self._timeframes:
            cfg = TF_CONFIGS.get(tf)
            if cfg is None:
                logger.warning('Unknown timeframe %s, skipping', tf)
                continue
            self._scheduler.add_job(
                self._run_scan_job,
                trigger            = CronTrigger(**cfg['cron']),
                id                 = f'scan_{tf}',
                name               = f'Pattern Scan {tf}',
                kwargs             = {'timeframe': tf, 'kline_bars': cfg['kline_bars']},
                replace_existing   = True,
                misfire_grace_time = cfg['stale_min'] * 60,
            )
            logger.info('Registered scan job: %s cron=%s bars=%d',
                        tf, cfg['cron'], cfg['kline_bars'])

        # 回测统计：每天凌晨2:30 UTC
        self._scheduler.add_job(
            self._run_backtest_job,
            trigger = CronTrigger(hour=2, minute=30),
            id      = 'backtest_daily',
            name    = 'Backtest Stats Daily',
            replace_existing = True,
        )

        # 检查 stale 任务：每10分钟
        self._scheduler.add_job(
            self._check_stale_jobs,
            trigger = IntervalTrigger(minutes=10),
            id      = 'stale_check',
            name    = 'Stale Job Check',
            replace_existing = True,
        )

        logger.info('Scheduler setup complete: %d jobs registered', len(self._scheduler.get_jobs()))

    async def start(self) -> None:
        """启动调度器和 Web 服务器（阻塞直到 stop() 被调用）"""
        init_db(self._db_url)
        self._scheduler.start()
        self._running = True
        logger.info('PatternScannerScheduler started')

        # 启动 Web 服务器（port 8082）
        try:
            self._web_runner = await start_web_server(
                host=self._web_host,
                port=self._web_port,
            )
        except Exception as e:
            logger.error('Failed to start web server: %s', e)
            self._web_runner = None

        try:
            while self._running:
                await asyncio.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            pass
        finally:
            await self.stop()

    async def stop(self) -> None:
        self._running = False
        if self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        if getattr(self, '_web_runner', None):
            await self._web_runner.cleanup()
        await dispose_engine()
        logger.info('PatternScannerScheduler stopped')

    # ──────────────────────────────────────────────────────────────────────────

    async def _run_scan_job(self, timeframe: str = '4h', kline_bars: int = 500) -> None:
        logger.info('Starting scan job [tf=%s bars=%d]', timeframe, kline_bars)
        try:
            summary = await run_full_pipeline(
                db_url        = self._db_url,
                api_key       = self._api_key,
                timeframe     = timeframe,
                kline_bars    = kline_bars,
                run_llm       = self._run_llm and bool(self._api_key),
                llm_model     = self._llm_model,
                llm_base_url  = self._llm_base_url,
            )
            logger.info(
                'Scan job done [tf=%s]: %d symbols scanned, %d patterns found',
                timeframe, summary.symbols_scannable, summary.patterns_found,
            )
        except Exception as e:
            logger.error('Scan job failed [tf=%s]: %s', timeframe, e, exc_info=True)

    async def _run_backtest_job(self) -> None:
        logger.info('Starting backtest stats job')
        try:
            from .backtest.stats_builder import BacktestStatsBuilder, BacktestConfig
            repo    = PatternRepository()
            builder = BacktestStatsBuilder(repo)
            kline_data = await _load_klines_from_db(repo, self._timeframe)
            await builder.build_all(kline_data, BacktestConfig())
            logger.info('Backtest stats job done')
        except Exception as e:
            logger.error('Backtest stats job failed: %s', e, exc_info=True)

    async def _check_stale_jobs(self) -> None:
        try:
            repo  = PatternRepository()
            stale = await repo.get_stale_jobs(stale_minutes=STALE_MINUTES)
            for log in stale:
                logger.warning('Marking stale job: %s', log.job_id)
                await repo.finish_job_log(
                    job_id        = log.job_id,
                    status        = 'failed',
                    error_message = f'Stale timeout after {STALE_MINUTES}min',
                )
        except Exception as e:
            logger.debug('Stale check error: %s', e)

    async def trigger_scan_now(self, timeframe: Optional[str] = None) -> None:
        """立即触发扫描（手动调用）：指定周期或全部周期"""
        tfs = [timeframe] if timeframe else self._timeframes
        for tf in tfs:
            cfg = TF_CONFIGS.get(tf, {'kline_bars': 500})
            await self._run_scan_job(timeframe=tf, kline_bars=cfg['kline_bars'])


async def _load_klines_from_db(
    repo: PatternRepository,
    timeframe: str,
    limit_per_symbol: int = 500,
) -> dict[str, 'pd.DataFrame']:
    import pandas as pd
    try:
        symbols = await repo.get_active_symbols()
        result: dict[str, pd.DataFrame] = {}
        for sym in symbols[:50]:
            try:
                df = await repo.get_klines(sym, timeframe, limit=limit_per_symbol)
                if not df.empty:
                    result[sym] = df
            except Exception:
                continue
        return result
    except Exception as e:
        logger.error('Failed to load klines from DB: %s', e)
        return {}


def main() -> None:
    import argparse

    logging.basicConfig(
        level  = logging.INFO,
        format = '%(asctime)s %(levelname)s [%(name)s] %(message)s',
    )

    parser = argparse.ArgumentParser(description='Pattern Scanner Scheduler')
    parser.add_argument('--db-url',     default=None)
    parser.add_argument('--api-key',    default=None)
    parser.add_argument('--timeframes', default=None,
                        help='逗号分隔的周期列表，如 4h,1h,15m（默认全部）')
    parser.add_argument('--no-llm',     action='store_true')
    parser.add_argument('--run-once',   action='store_true', help='只执行一次后退出')
    parser.add_argument('--run-once-tf', default=None,
                        help='run-once 时只扫描指定周期')
    args = parser.parse_args()

    tfs = [t.strip() for t in args.timeframes.split(',')] if args.timeframes else None

    scheduler = PatternScannerScheduler(
        db_url     = args.db_url,
        api_key    = args.api_key,
        timeframes = tfs,
        run_llm    = not args.no_llm,
    )

    if args.run_once:
        asyncio.run(scheduler.trigger_scan_now(timeframe=args.run_once_tf))
    else:
        scheduler.setup()
        asyncio.run(scheduler.start())


if __name__ == '__main__':
    main()
