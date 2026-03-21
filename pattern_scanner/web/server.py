"""
Pattern Scanner Web Server — aiohttp API + 静态文件服务
端口默认 8082
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

from aiohttp import web

from ..database.repository import PatternRepository
from ..database.session import get_session
from ..database.models import PatternScanResultORM
from ..patterns.definitions import ALL_PATTERNS, PATTERN_REGISTRY
from sqlalchemy import select, desc

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

            rows = (await session.execute(q)).scalars().all()

        data = [
            {
                'id':           r.id,
                'symbol':       r.symbol,
                'timeframe':    r.timeframe,
                'bar_time':     r.bar_time.isoformat() if r.bar_time else None,
                'pattern_id':   r.pattern_id,
                'pattern_name': r.pattern_name,
                'direction':    r.direction,
                'regime':       r.regime,
                'regime_score': r.regime_score,
                'total_score':  r.total_score,
                'trigger_met':  r.trigger_met,
                'llm_confidence': r.llm_confidence,
                'llm_enter_pool': r.llm_enter_pool,
                'llm_risk':     r.llm_risk,
                'llm_reasoning': r.llm_reasoning,
                'scan_batch_id': r.scan_batch_id,
                'created_at':   r.created_at.isoformat() if r.created_at else None,
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


async def handle_index(request: web.Request) -> web.Response:
    """GET / — 返回前端页面"""
    index = STATIC_DIR / 'index.html'
    return web.Response(
        text        = index.read_text(encoding='utf-8'),
        content_type = 'text/html',
    )


# ── 启动 ──────────────────────────────────────────────────────────────────────

def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get('/',              handle_index)
    app.router.add_get('/api/patterns',  handle_patterns)
    app.router.add_get('/api/results',   handle_results)
    app.router.add_get('/api/stats',     handle_stats)
    app.router.add_static('/static',     STATIC_DIR)
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
