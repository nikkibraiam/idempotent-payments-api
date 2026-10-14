import asyncio
import os
import sys

import httpx
import pytest

from app.config import Settings
from app.main import create_app

if sys.platform == "win32":
    # psycopg's async mode does not support the Proactor loop.
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

DATABASE_URL = os.getenv("TEST_DATABASE_URL", "postgresql://postgres@localhost:5432/payments_test")


@pytest.fixture
async def app():
    app = create_app(Settings(database_url=DATABASE_URL, lock_timeout_seconds=30, pool_size=20))
    async with app.router.lifespan_context(app):
        async with app.state.pool.connection() as conn:
            await conn.execute("TRUNCATE ledger_entries, payments, idempotency_keys, provider_charges RESTART IDENTITY")
        yield app


@pytest.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.fixture
def provider(app):
    return app.state.provider


@pytest.fixture
def db(app):
    async def query(sql, *params):
        async with app.state.pool.connection() as conn:
            cur = await conn.execute(sql, params or None)
            return await cur.fetchall() if cur.description else []

    return query
