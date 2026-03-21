from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

_engine = None
_SessionFactory: async_sessionmaker | None = None


def init_db(database_url: str | None = None) -> None:
    global _engine, _SessionFactory
    url = database_url or os.environ['DATABASE_URL']
    connect_args = {}
    if 'sqlite' in url:
        connect_args = {'check_same_thread': False}
        _engine = create_async_engine(url, connect_args=connect_args)
    else:
        _engine = create_async_engine(
            url,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
    _SessionFactory = async_sessionmaker(_engine, expire_on_commit=False)


@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    if _SessionFactory is None:
        raise RuntimeError('Database not initialised. Call init_db() first.')
    async with _SessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


async def create_tables() -> None:
    from .models import Base
    if _engine is None:
        raise RuntimeError('Database not initialised. Call init_db() first.')
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def dispose_engine() -> None:
    if _engine is not None:
        await _engine.dispose()
