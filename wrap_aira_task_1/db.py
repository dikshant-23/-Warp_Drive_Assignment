"""
Database session management.

Two engines:
  engine     -> aira_admin (BYPASSRLS), used by Alembic
  app_engine -> app_user (subject to RLS), used by the API

SET LOCAL scopes the tenant variable to the current transaction only.
Cannot bleed into another request even with connection pooling.
"""

import os
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy import text

engine = create_async_engine(
    os.getenv("DATABASE_URL", "postgresql+asyncpg://aira_admin:secret@localhost:5432/aira"),
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
)

app_engine = create_async_engine(
    os.getenv("DATABASE_URL_APP", "postgresql+asyncpg://app_user:appuser_secret@localhost:5432/aira"),
    pool_size=20,
    max_overflow=10,
    pool_pre_ping=True,
)

AppSession = async_sessionmaker(app_engine, expire_on_commit=False)


@asynccontextmanager
async def get_session(tenant_id: str):
    async with AppSession() as session:
        await session.execute(
            text("SET LOCAL app.current_tenant = :tid"),
            {"tid": tenant_id},
        )
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise