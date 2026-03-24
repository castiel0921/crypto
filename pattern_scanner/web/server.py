"""
Pattern Scanner Web Server — aiohttp API + 静态文件服务
端口默认 8082
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from aiohttp import web

from ..database.repository import PatternRepository
from ..database.session import get_session
from ..database.models import PatternScanResultORM, PatternBacktestStatsORM, SymbolUniverseORM
from ..patterns.definitions import ALL_PATTERNS, PATTERN_REGISTRY
from sqlalchemy import select, desc, func

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / 'static'


# ── API handlers ──────────────────────────────────────────────────────────────

async def handle_patterns(request: web.Request) -> web.Response:
    """GET /api/patterns — 返回所有形态定义"""
    data = []
    for p in ALL_PATTERNS:
        confirm_fields  = [f for f in p.fields if f.field_type == 'confirm']
        exclude_fields  = [f for f in p.fields if f.field_type == 'exclude']
        trigger_fields  = [f for f in p.fields if f.field_type == 'trigger']
        data.append({
            'pattern_id':   p.pattern_id,
            'pattern_name': p.pattern_name,
            'category':     p.category,
            'direction':    p.direction,
            'timeframes':   p.timeframes,
            'min_bars':     p.min_bars,
            'score_pass':   p.score_pass,
            'score_high':   p.score_high,
            'regime_filter': p.regime_filter,
            'version':      p.version,
            'confirm_fields': [
                {
                    'field_id':   f.field_id,
                    'field_name': f.field_name,
                    'indicator':  f.indicator,
                    'operator':   f.operator,
                    'param_a':    f.param_a,
                    'param_b':    f.param_b,
                    'is_required': f.is_required,
                    'weight':     f.weight,
                    'description': f.description,
                }
                for f in confirm_fields
            ],
            'exclude_fields': [
                {
                    'field_id':   f.field_id,
                    'field_name': f.field_name,
                    'indicator':  f.indicator,
                    'description': f.description,
                }
                for f in exclude_fields
            ],
            'trigger_fields': [
                {
                    'field_id':   f.field_id,
                    'field_name': f.field_name,
                    'indicator':  f.indicator,
                    'description': f.description,
                }
                for f in trigger_fields
            ],
        })
    return _json(data)


async def handle_results(request: web.Request) -> web.Response:
    """GET /api/results — 最近扫描结果"""
    limit        = int(request.rel_url.query.get('limit', '100'))
    pattern_id   = request.rel_url.query.get('pattern_id')
    direction    = request.rel_url.query.get('direction')
    min_score    = float(request.rel_url.query.get('min_score', '0'))
    hours        = int(request.rel_url.query.get('hours', '24'))
    tf_filter    = request.rel_url.query.get('tf')  # '4h' / '1h' / '15m' / None

    since = datetime.utcnow() - timedelta(hours=hours)

    try:
        async with get_session() as session:
            q = (
                select(PatternScanResultORM)
                .where(
                    PatternScanResultORM.is_filter_hit == False,
                    PatternScanResultORM.total_score   >= min_score,
                    PatternScanResultORM.created_at    >= since,
                )
                .order_by(desc(PatternScanResultORM.created_at))
                .limit(limit)
            )
            if pattern_id:
                q = q.where(PatternScanResultORM.pattern_id == pattern_id)
            if direction:
                q = q.where(PatternScanResultORM.direction == direction)
            if tf_filter:
                q = q.where(PatternScanResultORM.timeframe == tf_filter)

            rows = (await session.execute(q)).scalars().all()

        data = [
            {
                'id':           r.id,
                'symbol':       r.symbol,
                'timeframe':    r.timeframe,
                'bar_time':     (r.bar_time.isoformat() + 'Z') if r.bar_time else None,
                'pattern_id':   r.pattern_id,
                'pattern_name': r.pattern_name,
                'direction':    r.direction,
                'regime':       r.regime,
                'regime_score': r.regime_score,
                'total_score':  r.total_score,
                'trigger_met':  r.trigger_met,
                'llm_confidence': r.llm_confidence,
                'llm_enter_pool': r.llm_enter_pool,
                'llm_risk':       r.llm_risk,
                'llm_reasoning':  r.llm_reasoning,
                'confirm_score':  r.confirm_score,
                'exclude_penalty': r.exclude_penalty,
                'trigger_type':   r.trigger_type,
                'field_results':  r.field_results,
                'raw_values':     r.raw_values,
                'scan_batch_id':  r.scan_batch_id,
                'created_at':     r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ]
        return _json({'results': data, 'count': len(data)})

    except Exception as e:
        logger.error('handle_results error: %s', e)
        return _json({'error': str(e)}, status=500)


async def handle_stats(request: web.Request) -> web.Response:
    """GET /api/stats — 汇总统计"""
    hours = int(request.rel_url.query.get('hours', '24'))
    since = datetime.utcnow() - timedelta(hours=hours)

    try:
        async with get_session() as session:
            all_rows = (await session.execute(
                select(PatternScanResultORM)
                .where(
                    PatternScanResultORM.is_filter_hit == False,
                    PatternScanResultORM.created_at    >= since,
                )
            )).scalars().all()

        by_pattern: dict[str, int] = {}
        by_direction = {'long': 0, 'short': 0, 'neutral': 0}
        high_quality = 0
        triggered    = 0

        for r in all_rows:
            by_pattern[r.pattern_id] = by_pattern.get(r.pattern_id, 0) + 1
            if r.direction in by_direction:
                by_direction[r.direction] += 1
            if r.total_score and r.total_score >= 85:
                high_quality += 1
            if r.trigger_met:
                triggered += 1

        return _json({
            'period_hours':  hours,
            'total':         len(all_rows),
            'high_quality':  high_quality,
            'triggered':     triggered,
            'by_pattern':    by_pattern,
            'by_direction':  by_direction,
        })

    except Exception as e:
        logger.error('handle_stats error: %s', e)
        return _json({'error': str(e)}, status=500)


async def handle_scan_history(request: web.Request) -> web.Response:
    """GET /api/scan-history — 历史命中记录，含远期收益"""
    pattern_id   = request.rel_url.query.get('pattern_id')
    symbol       = request.rel_url.query.get('symbol')
    regime       = request.rel_url.query.get('regime')
    days         = int(request.rel_url.query.get('days', '30'))
    forward_bars = int(request.rel_url.query.get('forward_bars', '12'))
    limit        = int(request.rel_url.query.get('limit', '300'))
    since        = datetime.utcnow() - timedelta(days=days)

    try:
        async with get_session() as session:
            q = (
                select(PatternScanResultORM)
                .where(
                    PatternScanResultORM.is_filter_hit == False,
                    PatternScanResultORM.bar_time      >= since,
                )
                .order_by(desc(PatternScanResultORM.bar_time))
                .limit(limit)
            )
            if pattern_id: q = q.where(PatternScanResultORM.pattern_id == pattern_id)
            if symbol:     q = q.where(PatternScanResultORM.symbol     == symbol)
            if regime:     q = q.where(PatternScanResultORM.regime     == regime)
            rows = (await session.execute(q)).scalars().all()

        # 批量加载每个 symbol 的 kline 数据，用于计算远期收益
        from ..database.models import KlineCacheORM
        symbols_needed = list({r.symbol for r in rows})
        kline_map: dict[str, list] = {}
        async with get_session() as session:
            for sym in symbols_needed:
                krows = (await session.execute(
                    select(KlineCacheORM)
                    .where(KlineCacheORM.symbol == sym, KlineCacheORM.interval == '4h')
                    .order_by(KlineCacheORM.open_time.asc())
                )).scalars().all()
                kline_map[sym] = [(r.open_time, r.close) for r in krows]

        import bisect

        def calc_fwd(sym, bar_time, n, direction):
            klines = kline_map.get(sym, [])
            if not klines:
                return None
            times = [k[0] for k in klines]
            bt = bar_time.replace(tzinfo=None) if hasattr(bar_time, 'tzinfo') and bar_time.tzinfo else bar_time
            idx = bisect.bisect_left(times, bt)
            if idx < 0 or idx + n >= len(klines):
                return None
            entry = klines[idx][1]
            exit_ = klines[idx + n][1]
            if not entry:
                return None
            ret = (exit_ - entry) / entry
            return round(-ret if direction == 'short' else ret, 4)

        data = []
        for r in rows:
            fwd = calc_fwd(r.symbol, r.bar_time, forward_bars, r.direction or 'long')
            data.append({
                'id':           r.id,
                'symbol':       r.symbol,
                'pattern_id':   r.pattern_id,
                'pattern_name': r.pattern_name,
                'direction':    r.direction,
                'regime':       r.regime,
                'total_score':  r.total_score,
                'trigger_met':  r.trigger_met,
                'llm_confidence': r.llm_confidence,
                'bar_time':     (r.bar_time.isoformat() + 'Z') if r.bar_time else None,
                'forward_return': fwd,
                'forward_bars': forward_bars,
            })

        # 聚合统计
        valid = [d for d in data if d['forward_return'] is not None]
        win_threshold = 0.01
        summary = {}
        if valid:
            rets = [d['forward_return'] for d in valid]
            summary = {
                'sample':    len(valid),
                'win_rate':  round(sum(1 for r in rets if r > win_threshold) / len(rets), 3),
                'avg_return': round(sum(rets) / len(rets), 4),
                'max_return': round(max(rets), 4),
                'min_return': round(min(rets), 4),
            }

        return _json({'hits': data, 'count': len(data), 'summary': summary})
    except Exception as e:
        logger.error('handle_scan_history error: %s', e, exc_info=True)
        return _json({'error': str(e)}, status=500)


async def handle_run_backtest(request: web.Request) -> web.Response:
    """POST /api/run-backtest — 手动触发历史回测统计"""
    try:
        from ..backtest.stats_builder import BacktestStatsBuilder, BacktestConfig
        from ..database.models import KlineCacheORM
        import pandas as pd

        async with get_session() as session:
            syms_result = await session.execute(
                select(KlineCacheORM.symbol).distinct()
            )
            symbols = [r[0] for r in syms_result.fetchall()]

        kline_data: dict[str, pd.DataFrame] = {}
        async with get_session() as session:
            for sym in symbols[:100]:
                krows = (await session.execute(
                    select(KlineCacheORM)
                    .where(KlineCacheORM.symbol == sym, KlineCacheORM.interval == '4h')
                    .order_by(KlineCacheORM.open_time.asc())
                )).scalars().all()
                if len(krows) >= 50:
                    import pandas as pd
                    df = pd.DataFrame([{
                        'open':   r.open,  'high': r.high,
                        'low':    r.low,   'close': r.close,
                        'volume': r.volume,
                    } for r in krows],
                    index=pd.DatetimeIndex([r.open_time for r in krows]))
                    kline_data[sym] = df

        from ..database.repository import PatternRepository
        repo  = PatternRepository()
        builder = BacktestStatsBuilder(repo)
        all_stats = await builder.build_all(kline_data, BacktestConfig())
        return _json({'status': 'ok', 'groups_computed': len(all_stats), 'symbols': len(kline_data)})
    except Exception as e:
        logger.error('handle_run_backtest error: %s', e, exc_info=True)
        return _json({'error': str(e)}, status=500)


async def handle_backtest(request: web.Request) -> web.Response:
    """GET /api/backtest — 回测统计，按形态汇总胜率/收益"""
    try:
        async with get_session() as session:
            rows = (await session.execute(
                select(PatternBacktestStatsORM)
                .order_by(PatternBacktestStatsORM.pattern_id, PatternBacktestStatsORM.regime)
            )).scalars().all()

        data = [
            {
                'pattern_id':    r.pattern_id,
                'regime':        r.regime,
                'timeframe':     r.timeframe,
                'forward_bars':  r.forward_bars,
                'sample_size':   r.sample_size,
                'win_rate':      r.win_rate,
                'avg_return':    r.avg_return,
                'max_drawdown':  r.max_drawdown,
                'sharpe_like':   r.sharpe_like,
                'stat_period_start': r.stat_period_start.isoformat() if r.stat_period_start else None,
                'stat_period_end':   r.stat_period_end.isoformat()   if r.stat_period_end   else None,
            }
            for r in rows
        ]
        return _json({'backtest': data, 'count': len(data)})
    except Exception as e:
        logger.error('handle_backtest error: %s', e)
        return _json({'error': str(e)}, status=500)


async def handle_regime_stats(request: web.Request) -> web.Response:
    """GET /api/regime-stats — 近期结果体制分布 + 强弱分布"""
    hours = int(request.rel_url.query.get('hours', '168'))
    since = datetime.utcnow() - timedelta(hours=hours)
    try:
        async with get_session() as session:
            rows = (await session.execute(
                select(PatternScanResultORM).where(
                    PatternScanResultORM.is_filter_hit == False,
                    PatternScanResultORM.created_at    >= since,
                )
            )).scalars().all()

        by_regime: dict[str, int] = {}
        pattern_scores: dict[str, list[float]] = {}
        for r in rows:
            reg = r.regime or 'unknown'
            by_regime[reg] = by_regime.get(reg, 0) + 1
            if r.pattern_id and r.total_score:
                pattern_scores.setdefault(r.pattern_id, []).append(r.total_score)

        avg_scores = {
            pid: round(sum(scores) / len(scores), 1)
            for pid, scores in pattern_scores.items()
        }
        return _json({
            'period_hours': hours,
            'by_regime':    by_regime,
            'avg_score_by_pattern': avg_scores,
        })
    except Exception as e:
        logger.error('handle_regime_stats error: %s', e)
        return _json({'error': str(e)}, status=500)


async def handle_index(request: web.Request) -> web.Response:
    """GET / — 返回前端页面"""
    index = STATIC_DIR / 'index.html'
    return web.Response(
        text        = index.read_text(encoding='utf-8'),
        content_type = 'text/html',
    )


async def handle_analyze(request: web.Request) -> web.Response:
    """POST /api/analyze — 对指定交易对+时间段进行结构诊断（指标+LLM）"""
    try:
        body = await request.json()
    except Exception:
        body = {}

    raw_sym = body.get('symbol', '').strip().upper().replace('/', '').replace('-', '')
    if not raw_sym:
        return _json({'error': 'symbol required'}, status=400)
    symbol    = raw_sym if raw_sym.endswith('USDT') else raw_sym + 'USDT'
    timeframe = body.get('timeframe', '4h')
    lookback  = max(30, min(int(body.get('lookback_bars', 100)), 500))
    end_str   = body.get('end_time')

    end_dt = None
    if end_str:
        try:
            end_dt = datetime.fromisoformat(end_str.replace('Z', ''))
        except Exception:
            pass

    try:
        from ..database.models import KlineCacheORM
        import pandas as pd

        async with get_session() as session:
            q = (
                select(KlineCacheORM)
                .where(KlineCacheORM.symbol == symbol, KlineCacheORM.interval == timeframe)
                .order_by(KlineCacheORM.open_time.asc())
            )
            if end_dt:
                q = q.where(KlineCacheORM.open_time <= end_dt)
            krows = (await session.execute(q)).scalars().all()

        if not krows:
            # DB 无缓存，直接从 Binance 拉取
            from ..data.fetcher import BinanceFetcher
            fetcher = BinanceFetcher()
            try:
                end_time_ms = int(end_dt.timestamp() * 1000) if end_dt else None
                df = await fetcher.fetch_klines(
                    symbol, timeframe,
                    limit=lookback + 200,
                    end_time=end_time_ms,
                )
            finally:
                await fetcher.close()
            if df.empty:
                return _json({'error': f'No kline data for {symbol}/{timeframe}'}, status=404)
            # fetch_klines 返回以 open_time(ms) 为 index 的 DataFrame，转为 datetime index
            df.index = pd.to_datetime(df.index, unit='ms', utc=True).tz_localize(None)
        else:
            # 保留末尾 lookback+200 条（多余的用于指标计算回溯）
            krows = list(krows)[-(lookback + 200):]
            df = pd.DataFrame([{
                'open': r.open, 'high': r.high, 'low': r.low,
                'close': r.close, 'volume': r.volume,
            } for r in krows], index=pd.DatetimeIndex([r.open_time for r in krows]))

        if len(df) < 30:
            return _json({'error': f'Insufficient data: {len(df)} bars'}, status=400)

        from ..scanner import PatternScanner
        from ..indicators import IndicatorLibrary
        from ..field_evaluator import FieldEvaluator

        scanner       = PatternScanner(patterns=ALL_PATTERNS)
        regime_result = scanner.regime_detector.detect(df)
        local_ind     = IndicatorLibrary()
        evaluator     = FieldEvaluator(local_ind)

        # 对所有 A/B 类形态打分（不过滤阈值）
        pattern_scores = []
        for pattern in ALL_PATTERNS:
            if pattern.category == 'C':
                continue
            if len(df) < pattern.min_bars:
                continue
            r = scanner.score_pattern(df, pattern, regime_result, evaluator, symbol, timeframe)
            pattern_scores.append({
                'pattern_id':      r.pattern_id,
                'pattern_name':    r.pattern_name,
                'direction':       r.direction,
                'total_score':     r.total_score,
                'confirm_score':   r.confirm_score,
                'exclude_penalty': r.exclude_penalty,
                'trigger_met':     r.trigger_met,
                'field_results':   r.field_results,
                'raw_values':      {k: round(v, 4) for k, v in r.raw_values.items()},
            })

        pattern_scores.sort(key=lambda x: x['total_score'], reverse=True)
        top = pattern_scores[0] if pattern_scores else None

        # krows 可能为空（Binance 直拉路径），改从 df.index 取时间
        if krows:
            start_bar = krows[-lookback].open_time if len(krows) >= lookback else krows[0].open_time
            end_bar   = krows[-1].open_time
        else:
            idx = df.index
            start_bar = idx[-lookback].to_pydatetime() if len(idx) >= lookback else idx[0].to_pydatetime()
            end_bar   = idx[-1].to_pydatetime()

        # LLM 结构诊断（可选）
        llm_analysis = None
        api_key      = os.environ.get('DEEPSEEK_API_KEY', '')
        llm_base_url = os.environ.get('LLM_BASE_URL', 'https://api.deepseek.com')
        llm_model    = os.environ.get('LLM_MODEL', 'deepseek-chat')
        if api_key:
            try:
                from ..llm.base import LLMClient
                client = LLMClient(api_key=api_key, base_url=llm_base_url, model=llm_model)
                llm_analysis = await _llm_structure_diagnosis(
                    client, symbol, timeframe, regime_result,
                    pattern_scores, start_bar, end_bar, lookback,
                )
            except Exception as e:
                logger.warning('LLM structure diagnosis failed: %s', e)
                llm_analysis = {'error': str(e)}

        # 返回最后 lookback 根 K 线数据供前端绘图
        plot_df = df.iloc[-lookback:]
        klines_data = [
            {
                'time':   int(ts.timestamp()),
                'open':   round(float(row.open),   6),
                'high':   round(float(row.high),   6),
                'low':    round(float(row.low),    6),
                'close':  round(float(row.close),  6),
                'volume': round(float(row.volume), 2),
            }
            for ts, row in plot_df.iterrows()
        ]

        return _json({
            'symbol':         symbol,
            'timeframe':      timeframe,
            'bars_analyzed':  lookback,
            'start_bar':      (start_bar.isoformat() + 'Z') if start_bar else None,
            'end_bar':        (end_bar.isoformat()   + 'Z') if end_bar   else None,
            'regime':         regime_result.regime.value,
            'regime_score':   regime_result.score,
            'trend_score':    regime_result.trend_score,
            'vol_score':      regime_result.vol_score,
            'regime_meta':    {k: (round(v, 4) if isinstance(v, float) else v)
                               for k, v in regime_result.meta.items()},
            'pattern_scores': pattern_scores,
            'top_pattern':    top,
            'llm_analysis':   llm_analysis,
            'klines':         klines_data,
        })
    except Exception as e:
        logger.error('handle_analyze error: %s', e, exc_info=True)
        return _json({'error': str(e)}, status=500)


async def _llm_structure_diagnosis(
    client, symbol, timeframe, regime_result,
    pattern_scores, start_bar, end_bar, lookback,
) -> dict:
    """对K线段调用 LLM 做开放式结构诊断"""
    system_prompt = (
        "你是一个专业的加密货币技术分析师。给定一段K线的量化分析数据，"
        "请判断这段K线属于什么市场结构，并做出综合评估。\n"
        "只能返回合法的JSON对象，不能有任何额外文字。\n"
        "返回格式：\n"
        "{\n"
        '  "structure_type": "主要结构类型（如：上升后平台整理、底部累积、下降趋势延续、'
        '震荡三角、楔形整理等）",\n'
        '  "structure_label": "简短标签（≤6字）",\n'
        '  "confidence": "high/medium/low",\n'
        '  "key_features": ["最重要的3-5个结构特征"],\n'
        '  "trend_assessment": "当前趋势方向和强度（1-2句）",\n'
        '  "volume_assessment": "量能特征（1句）",\n'
        '  "next_scenario": "最可能的后续发展（1-2句）",\n'
        '  "reasoning": "综合分析（2-3句）"\n'
        "}"
    )

    top3 = [
        f"{p['pattern_id']} {p['pattern_name']} ({p['direction']}) 得分={p['total_score']:.0f} "
        f"{'[已触发]' if p['trigger_met'] else ''}"
        for p in pattern_scores[:3]
    ]
    key_ind_keys = ('max_advance_45', 'max_advance_40', 'platform_range_ratio',
                    'pullback_ratio', 'atr_ratio', 'rsi14', 'bb_width')
    key_raw = {}
    if pattern_scores:
        key_raw = {k: v for k, v in pattern_scores[0]['raw_values'].items()
                   if k in key_ind_keys}

    payload = {
        'symbol':          symbol,
        'timeframe':       timeframe,
        'analysis_period': (
            f"{start_bar.strftime('%Y-%m-%d %H:%M')} → "
            f"{end_bar.strftime('%Y-%m-%d %H:%M')} ({lookback}根K线)"
        ),
        'market_regime':   regime_result.regime.value,
        'regime_score':    regime_result.score,
        'trend_score':     regime_result.trend_score,
        'vol_score':       regime_result.vol_score,
        'top3_pattern_scores': top3,
        'key_indicators':  key_raw,
        'regime_meta':     {k: (round(v, 4) if isinstance(v, float) else v)
                            for k, v in regime_result.meta.items()},
    }

    return await client.chat_json(system_prompt, json.dumps(payload, ensure_ascii=False))


# ── 启动 ──────────────────────────────────────────────────────────────────────

async def handle_symbols(request: web.Request) -> web.Response:
    """GET /api/symbols — 返回活跃交易对列表（用于诊断页下拉）"""
    async with get_session() as session:
        result = await session.execute(
            select(SymbolUniverseORM.symbol)
            .where(SymbolUniverseORM.is_active == True)
            .order_by(SymbolUniverseORM.symbol.asc())
        )
        symbols = [r[0] for r in result.all()]
    return _json({'symbols': symbols})


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get('/',                  handle_index)
    app.router.add_get('/api/patterns',      handle_patterns)
    app.router.add_get('/api/results',       handle_results)
    app.router.add_get('/api/stats',         handle_stats)
    app.router.add_get('/api/backtest',       handle_backtest)
    app.router.add_get('/api/regime-stats',  handle_regime_stats)
    app.router.add_get('/api/scan-history',  handle_scan_history)
    app.router.add_get('/api/symbols',       handle_symbols)
    app.router.add_post('/api/run-backtest', handle_run_backtest)
    app.router.add_post('/api/analyze',      handle_analyze)
    app.router.add_static('/static',         STATIC_DIR)
    return app


async def start_web_server(
    host: str = '0.0.0.0',
    port: int = 8082,
) -> web.AppRunner:
    app    = create_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host, port)
    await site.start()
    logger.info('Pattern Scanner web server started at http://%s:%d', host, port)
    return runner


def _json(data, status: int = 200) -> web.Response:
    return web.Response(
        text         = json.dumps(data, ensure_ascii=False, default=str),
        content_type = 'application/json',
        status       = status,
    )
