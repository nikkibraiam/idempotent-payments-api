"""Payment creation with idempotency keys and atomic phases.

A request is split into phases. Each phase either runs entirely inside one
database transaction, or is a single call to the external provider. After every
phase the idempotency key's ``recovery_point`` is advanced in the same
transaction, so a retry with the same key resumes exactly where the previous
attempt stopped:

    started ──(tx: insert payment)──▶ payment_created
            ──(provider.charge, idempotent on its side)──▶
            ──(tx: settle payment + ledger + store response)──▶ finished

The provider call is the only non-transactional step. It is made safe by
sending the provider a key derived from the payment id, which is fixed once
``payment_created`` is committed: no matter how many times it is retried, the
customer is charged at most once.
"""

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from psycopg import AsyncConnection
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .provider import FakeProvider, ProviderDeclined, ProviderUnavailable
from .schemas import CreatePayment


@dataclass
class Result:
    status: int
    body: dict[str, Any]
    replayed: bool = False
    retry_after: int | None = None


@dataclass
class LockedKey:
    id: int
    recovery_point: str


def request_fingerprint(method: str, path: str, body: dict[str, Any]) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{method} {path}\n{canonical}".encode()).hexdigest()


def serialize_payment(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "status": row["status"],
        "amount_minor": row["amount_minor"],
        "currency": row["currency"],
        "source_account": row["source_account"],
        "destination_account": row["destination_account"],
        "provider_charge_id": row["provider_charge_id"],
        "failure_reason": row["failure_reason"],
        "created_at": row["created_at"].isoformat(),
    }


class PaymentService:
    def __init__(self, pool: AsyncConnectionPool, provider: FakeProvider, lock_timeout_seconds: float):
        self._pool = pool
        self._provider = provider
        self._lock_timeout = lock_timeout_seconds

    async def create_payment(self, client_id: str, key: str, req: CreatePayment) -> Result:
        fingerprint = request_fingerprint("POST", "/v1/payments", req.model_dump(mode="json"))
        acquired = await self._acquire_key(client_id, key, fingerprint)
        if isinstance(acquired, Result):
            return acquired

        try:
            return await self._run_phases(acquired, client_id, req)
        except ProviderUnavailable:
            # Unknown outcome. Keep the recovery point, free the key and let the
            # client retry with the same key; the provider's own idempotency
            # makes the repeated charge call safe.
            await self._release_key(acquired.id)
            return Result(
                503,
                {"error": "provider_unavailable", "message": "Retry with the same Idempotency-Key."},
                retry_after=1,
            )
        except BaseException:
            await self._release_key(acquired.id)
            raise

    async def get_payment(self, client_id: str, payment_id: str) -> dict[str, Any] | None:
        async with self._pool.connection() as conn:
            cur = await conn.execute("SELECT * FROM payments WHERE id = %s AND client_id = %s", (payment_id, client_id))
            row = await cur.fetchone()
        return serialize_payment(row) if row else None

    async def _acquire_key(self, client_id: str, key: str, fingerprint: str) -> LockedKey | Result:
        async with self._pool.connection() as conn, conn.transaction():
            # Concurrent inserts of the same key block on the unique index until
            # the first transaction commits, then fall through to the SELECT.
            cur = await conn.execute(
                """
                INSERT INTO idempotency_keys (client_id, key, request_hash, locked_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (client_id, key) DO NOTHING
                RETURNING id, recovery_point
                """,
                (client_id, key, fingerprint),
            )
            row = await cur.fetchone()
            if row:
                return LockedKey(row["id"], row["recovery_point"])

            cur = await conn.execute(
                """
                SELECT id, request_hash, recovery_point, response_code, response_body,
                       locked_at > now() - make_interval(secs => %s) AS lock_held
                FROM idempotency_keys
                WHERE client_id = %s AND key = %s
                FOR UPDATE
                """,
                (self._lock_timeout, client_id, key),
            )
            row = await cur.fetchone()

            if row["request_hash"] != fingerprint:
                return Result(
                    422,
                    {
                        "error": "idempotency_key_reused",
                        "message": "This Idempotency-Key was already used with different parameters.",
                    },
                )
            if row["recovery_point"] == "finished":
                return Result(row["response_code"], row["response_body"], replayed=True)
            if row["lock_held"]:
                return Result(
                    409,
                    {"error": "request_in_progress", "message": "A request with this key is still running."},
                    retry_after=1,
                )

            # Unlocked (previous attempt failed transiently) or the lock expired
            # (previous attempt crashed): take over and resume.
            await conn.execute(
                "UPDATE idempotency_keys SET locked_at = now(), last_run_at = now() WHERE id = %s",
                (row["id"],),
            )
            return LockedKey(row["id"], row["recovery_point"])

    async def _run_phases(self, key: LockedKey, client_id: str, req: CreatePayment) -> Result:
        if key.recovery_point == "started":
            async with self._pool.connection() as conn, conn.transaction():
                await conn.execute(
                    """
                    INSERT INTO payments (idempotency_key_id, client_id, amount_minor, currency,
                                          source_account, destination_account)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (key.id, client_id, req.amount_minor, req.currency, req.source_account, req.destination_account),
                )
                await self._advance(conn, key.id, "payment_created")
            key.recovery_point = "payment_created"

        if key.recovery_point == "payment_created":
            async with self._pool.connection() as conn:
                cur = await conn.execute("SELECT * FROM payments WHERE idempotency_key_id = %s", (key.id,))
                payment = await cur.fetchone()

            try:
                charge_id = await self._provider.charge(
                    idempotency_key=f"payment-{payment['id']}",
                    amount_minor=payment["amount_minor"],
                    currency=payment["currency"],
                )
            except ProviderDeclined as exc:
                return await self._settle(key.id, payment, succeeded=False, failure_reason=exc.reason)
            return await self._settle(key.id, payment, succeeded=True, charge_id=charge_id)

        raise RuntimeError(f"unknown recovery point {key.recovery_point!r}")

    async def _settle(
        self,
        key_id: int,
        payment: dict[str, Any],
        *,
        succeeded: bool,
        charge_id: str | None = None,
        failure_reason: str | None = None,
    ) -> Result:
        status_code = 201 if succeeded else 402
        async with self._pool.connection() as conn, conn.transaction():
            # Fence: if a slow attempt and a takeover both reach this point, only
            # the first one settles; the other returns the stored response.
            cur = await conn.execute(
                "SELECT recovery_point, response_code, response_body FROM idempotency_keys WHERE id = %s FOR UPDATE",
                (key_id,),
            )
            current = await cur.fetchone()
            if current["recovery_point"] == "finished":
                return Result(current["response_code"], current["response_body"], replayed=True)

            cur = await conn.execute(
                """
                UPDATE payments
                SET status = %s, provider_charge_id = %s, failure_reason = %s, updated_at = now()
                WHERE id = %s
                RETURNING *
                """,
                ("succeeded" if succeeded else "failed", charge_id, failure_reason, payment["id"]),
            )
            settled = await cur.fetchone()

            if succeeded:
                await conn.execute(
                    """
                    INSERT INTO ledger_entries (payment_id, account, amount_minor, currency)
                    VALUES (%(id)s, %(src)s, -%(amt)s, %(cur)s), (%(id)s, %(dst)s, %(amt)s, %(cur)s)
                    """,
                    {
                        "id": payment["id"],
                        "src": payment["source_account"],
                        "dst": payment["destination_account"],
                        "amt": payment["amount_minor"],
                        "cur": payment["currency"],
                    },
                )

            body = serialize_payment(settled)
            await conn.execute(
                """
                UPDATE idempotency_keys
                SET recovery_point = 'finished', response_code = %s, response_body = %s, locked_at = NULL
                WHERE id = %s
                """,
                (status_code, Jsonb(body), key_id),
            )
        return Result(status_code, body)

    @staticmethod
    async def _advance(conn: AsyncConnection, key_id: int, point: str) -> None:
        await conn.execute("UPDATE idempotency_keys SET recovery_point = %s WHERE id = %s", (point, key_id))

    async def _release_key(self, key_id: int) -> None:
        async with self._pool.connection() as conn:
            await conn.execute("UPDATE idempotency_keys SET locked_at = NULL WHERE id = %s", (key_id,))
