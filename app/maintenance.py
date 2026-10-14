from datetime import timedelta

from psycopg_pool import AsyncConnectionPool


async def purge_expired_keys(pool: AsyncConnectionPool, older_than: timedelta = timedelta(hours=24)) -> int:
    """Delete finished keys past the retention window.

    After this, reusing an old key creates a new payment, so the window must be
    longer than any client's retry horizon. Unfinished keys are kept: they
    still point at work that a retry may need to resume.
    """
    async with pool.connection() as conn:
        cur = await conn.execute(
            "DELETE FROM idempotency_keys WHERE recovery_point = 'finished' AND created_at < now() - %s",
            (older_than,),
        )
        return cur.rowcount
