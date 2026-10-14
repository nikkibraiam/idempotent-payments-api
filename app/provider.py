"""Simulated external payment provider.

It behaves like real providers (Stripe, Circle, BitGo...): it accepts its own
idempotency key, so retrying a charge with the same key returns the original
charge instead of creating a second one. Faults can be injected to reproduce
the failure modes the API has to survive.
"""

import asyncio
from collections import deque
from enum import StrEnum

from psycopg_pool import AsyncConnectionPool


class Fault(StrEnum):
    # Rejected for a business reason; retrying will not help.
    DECLINE = "decline"
    # Connection failed before the provider saw the request.
    UNAVAILABLE = "unavailable"
    # The provider charged, but the response never arrived. The dangerous one:
    # the caller cannot tell it apart from UNAVAILABLE.
    TIMEOUT_AFTER_CHARGE = "timeout_after_charge"


class ProviderDeclined(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class ProviderUnavailable(Exception):
    """Transient error: the charge may or may not have happened."""


class FakeProvider:
    def __init__(self, pool: AsyncConnectionPool):
        self._pool = pool
        self._faults: deque[Fault] = deque()
        self.latency_seconds = 0.0

    def inject(self, *faults: Fault) -> None:
        """Queue faults; each charge() call consumes one."""
        self._faults.extend(faults)

    async def charge(self, *, idempotency_key: str, amount_minor: int, currency: str) -> str:
        fault = self._faults.popleft() if self._faults else None
        if self.latency_seconds:
            await asyncio.sleep(self.latency_seconds)

        if fault is Fault.UNAVAILABLE:
            raise ProviderUnavailable("connection refused")
        if fault is Fault.DECLINE:
            raise ProviderDeclined("insufficient_funds")

        async with self._pool.connection() as conn:
            cur = await conn.execute(
                """
                INSERT INTO provider_charges (idempotency_key, amount_minor, currency)
                VALUES (%s, %s, %s)
                ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
                RETURNING id
                """,
                (idempotency_key, amount_minor, currency),
            )
            charge_id = (await cur.fetchone())["id"]

        if fault is Fault.TIMEOUT_AFTER_CHARGE:
            raise ProviderUnavailable("read timeout")
        return charge_id
