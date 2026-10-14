import asyncio
from datetime import timedelta

from app.maintenance import purge_expired_keys
from app.provider import Fault

PAYMENT = {
    "amount_minor": 12_500,
    "currency": "USD",
    "source_account": "acct_alice",
    "destination_account": "acct_bob",
}


def headers(key="key-1", client="client-a"):
    return {"Idempotency-Key": key, "X-Client-Id": client}


async def count(db, table):
    return (await db(f"SELECT count(*) AS n FROM {table}"))[0]["n"]


async def test_creates_payment_and_balanced_ledger(client, db):
    r = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert r.status_code == 201
    assert r.json()["status"] == "succeeded"
    assert "Idempotent-Replayed" not in r.headers
    entries = await db("SELECT account, amount_minor FROM ledger_entries ORDER BY amount_minor")
    assert [(e["account"], e["amount_minor"]) for e in entries] == [
        ("acct_alice", -12_500),
        ("acct_bob", 12_500),
    ]


async def test_retry_with_same_key_replays_original_response(client, db):
    first = await client.post("/v1/payments", json=PAYMENT, headers=headers())
    second = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert second.status_code == 201
    assert second.json() == first.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(db, "payments") == 1
    assert await count(db, "provider_charges") == 1


async def test_same_key_with_different_body_is_rejected(client, db):
    await client.post("/v1/payments", json=PAYMENT, headers=headers())
    r = await client.post("/v1/payments", json={**PAYMENT, "amount_minor": 99}, headers=headers())

    assert r.status_code == 422
    assert r.json()["error"] == "idempotency_key_reused"
    assert await count(db, "payments") == 1


async def test_keys_are_scoped_per_client(client, db):
    a = await client.post("/v1/payments", json=PAYMENT, headers=headers(client="client-a"))
    b = await client.post("/v1/payments", json=PAYMENT, headers=headers(client="client-b"))

    assert a.status_code == b.status_code == 201
    assert a.json()["id"] != b.json()["id"]
    assert await count(db, "payments") == 2


async def test_missing_key_is_rejected(client, db):
    r = await client.post("/v1/payments", json=PAYMENT, headers={"X-Client-Id": "client-a"})

    assert r.status_code == 400
    assert await count(db, "payments") == 0


async def test_concurrent_duplicates_charge_exactly_once(client, provider, db):
    provider.latency_seconds = 0.3

    responses = await asyncio.gather(*(client.post("/v1/payments", json=PAYMENT, headers=headers()) for _ in range(20)))

    statuses = sorted(r.status_code for r in responses)
    assert statuses.count(201) >= 1
    assert set(statuses) <= {201, 409}
    assert all(r.headers.get("Retry-After") == "1" for r in responses if r.status_code == 409)
    assert await count(db, "payments") == 1
    assert await count(db, "provider_charges") == 1
    assert await count(db, "ledger_entries") == 2


async def test_timeout_after_provider_charged_is_recovered_without_double_charge(client, provider, db):
    provider.inject(Fault.TIMEOUT_AFTER_CHARGE)

    first = await client.post("/v1/payments", json=PAYMENT, headers=headers())
    assert first.status_code == 503
    assert first.headers["Retry-After"] == "1"
    # The provider did charge, but we could not know it.
    assert await count(db, "provider_charges") == 1
    assert (await db("SELECT status FROM payments"))[0]["status"] == "pending"

    retry = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert retry.status_code == 201
    assert retry.json()["status"] == "succeeded"
    assert await count(db, "provider_charges") == 1
    assert await count(db, "payments") == 1
    assert await count(db, "ledger_entries") == 2


async def test_provider_unavailable_then_retry_succeeds(client, provider, db):
    provider.inject(Fault.UNAVAILABLE)

    assert (await client.post("/v1/payments", json=PAYMENT, headers=headers())).status_code == 503
    assert await count(db, "provider_charges") == 0

    retry = await client.post("/v1/payments", json=PAYMENT, headers=headers())
    assert retry.status_code == 201
    assert await count(db, "provider_charges") == 1


async def test_decline_is_final_and_replayed(client, provider, db):
    provider.inject(Fault.DECLINE)

    first = await client.post("/v1/payments", json=PAYMENT, headers=headers())
    second = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert first.status_code == second.status_code == 402
    assert first.json()["failure_reason"] == "insufficient_funds"
    assert second.headers["Idempotent-Replayed"] == "true"
    assert await count(db, "ledger_entries") == 0


async def test_request_from_crashed_worker_is_taken_over_after_lock_timeout(client, db):
    # Simulate a worker that created the payment and died before charging.
    await db(
        """
        WITH k AS (
            INSERT INTO idempotency_keys (client_id, key, request_hash, recovery_point, locked_at)
            VALUES ('client-a', 'key-1', %s, 'payment_created', now() - interval '5 minutes')
            RETURNING id
        )
        INSERT INTO payments (idempotency_key_id, client_id, amount_minor, currency,
                              source_account, destination_account)
        SELECT id, 'client-a', 12500, 'USD', 'acct_alice', 'acct_bob' FROM k
        RETURNING id
        """,
        _fingerprint(),
    )

    r = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert r.status_code == 201
    assert await count(db, "payments") == 1
    assert await count(db, "provider_charges") == 1


async def test_request_holding_a_fresh_lock_gets_409(client, db):
    await db(
        """
        INSERT INTO idempotency_keys (client_id, key, request_hash, locked_at)
        VALUES ('client-a', 'key-1', %s, now())
        """,
        _fingerprint(),
    )

    r = await client.post("/v1/payments", json=PAYMENT, headers=headers())

    assert r.status_code == 409
    assert r.json()["error"] == "request_in_progress"


async def test_purge_removes_only_old_finished_keys(app, client, db):
    await client.post("/v1/payments", json=PAYMENT, headers=headers("old"))
    await client.post("/v1/payments", json=PAYMENT, headers=headers("recent"))
    await db("UPDATE idempotency_keys SET created_at = now() - interval '2 days' WHERE key = 'old'")

    deleted = await purge_expired_keys(app.state.pool, older_than=timedelta(hours=24))

    assert deleted == 1
    assert [k["key"] for k in await db("SELECT key FROM idempotency_keys")] == ["recent"]
    assert await count(db, "payments") == 2


async def test_get_payment_is_scoped_per_client(client):
    created = (await client.post("/v1/payments", json=PAYMENT, headers=headers())).json()

    own = await client.get(f"/v1/payments/{created['id']}", headers={"X-Client-Id": "client-a"})
    other = await client.get(f"/v1/payments/{created['id']}", headers={"X-Client-Id": "client-b"})

    assert own.status_code == 200 and own.json() == created
    assert other.status_code == 404


def _fingerprint():
    from app.payments import request_fingerprint

    return request_fingerprint("POST", "/v1/payments", PAYMENT)
