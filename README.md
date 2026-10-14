# idempotent-payments-api

[![CI](https://github.com/nikkibraiam/idempotent-payments-api/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/nikkibraiam/idempotent-payments-api/actions/workflows/ci.yml)

A payments API that is **safe to retry**. Clients send an `Idempotency-Key`
with every `POST`; whatever happens (network drops, duplicate clicks,
concurrent retries, a provider that times out *after* charging, a worker that
crashes mid-request), the customer is charged **at most once** and every retry
gets the same answer.

FastAPI · PostgreSQL · psycopg 3 · no ORM: the locking is the point, so the SQL
is in plain sight.

## The problem

```
client ──POST /payments──▶ API ──charge──▶ provider
        ◀──── timeout ────        (charged? unknown)
client ──POST /payments──▶ API ──charge──▶ provider   ← double charge?
```

A unique constraint alone does not solve it. The dangerous step, calling an
external provider, cannot be part of a database transaction. So the request
has to be split into steps that can be resumed safely.

## Design: atomic phases + recovery points

```mermaid
stateDiagram-v2
    [*] --> started: insert key (locked)
    started --> payment_created: tx 1 - insert pending payment
    payment_created --> payment_created: provider timeout - 503, retry resumes here
    payment_created --> finished: charge provider, then tx 2 - settle, ledger, store response
    finished --> [*]: retries replay stored response
```

1. **Acquire the key.** `INSERT … ON CONFLICT (client_id, key) DO NOTHING`.
   Concurrent duplicates block on the unique index, then read the existing row
   `FOR UPDATE` and get one of:
   - **replay** of the stored response (`Idempotent-Replayed: true`), if finished;
   - **409 + `Retry-After`**, if another request holds a fresh lock;
   - **422**, if the same key comes with a different body (SHA-256 fingerprint);
   - **takeover**, if the lock expired (the previous worker crashed).
2. **Each phase is one transaction** that also advances `recovery_point`. A
   retry never repeats a committed phase.
3. **The provider call is made idempotent on the other side.** It uses a key
   derived from the payment id, and that id is fixed as soon as
   `payment_created` commits. If the provider times out after charging, the
   retry sends the same key and gets the same charge back.
4. **Settlement is fenced.** If a slow request and a takeover both reach the
   last phase, only the first one settles. The second one returns the stored
   response.
5. **Double-entry ledger.** Each successful payment writes two entries that
   sum to zero, in the same transaction that stores the response.

## Failure modes covered by tests

| Scenario | Expected | Test |
|---|---|---|
| Same key, same body | Stored response replayed, 1 charge | `test_retry_with_same_key_replays_original_response` |
| Same key, different body | 422 `idempotency_key_reused` | `test_same_key_with_different_body_is_rejected` |
| 20 concurrent duplicates | 1 payment, 1 charge, others 201/409 | `test_concurrent_duplicates_charge_exactly_once` |
| Provider charged, response lost | 503, retry → 201, still 1 charge | `test_timeout_after_provider_charged_is_recovered_without_double_charge` |
| Provider unreachable | 503, retry charges once | `test_provider_unavailable_then_retry_succeeds` |
| Card declined | 402, final, replayed | `test_decline_is_final_and_replayed` |
| Worker crashed holding the key | Next retry takes over after lock timeout | `test_request_from_crashed_worker_is_taken_over_after_lock_timeout` |
| Same key, different clients | Independent payments | `test_keys_are_scoped_per_client` |
| Key retention | Only old *finished* keys purged | `test_purge_removes_only_old_finished_keys` |

All tests run against a real PostgreSQL, not mocks: the guarantees depend on
its locking behaviour.

## API

```http
POST /v1/payments
Idempotency-Key: 6f1c2b8e-…
X-Client-Id: merchant_42
Content-Type: application/json

{"amount_minor": 12500, "currency": "USD", "source_account": "acct_alice", "destination_account": "acct_bob"}
```

| Status | Meaning |
|---|---|
| 201 | Payment succeeded (or replay of it) |
| 402 | Declined by provider (final, replayable) |
| 400 | Missing `Idempotency-Key` |
| 409 | Same key still being processed. Retry after `Retry-After` |
| 422 | Key reused with different parameters, or invalid body |
| 503 | Provider outcome unknown. Retry **with the same key** |

`GET /v1/payments/{id}` returns a payment owned by the calling client.

`X-Client-Id` stands in for the authenticated principal. In production it
comes from the auth layer, never from a client-controlled header.

## Run it

```bash
docker compose up --build          # API on http://localhost:8000/docs

# tests (needs a Postgres; compose's db works)
pip install -e ".[dev]"
TEST_DATABASE_URL=postgresql://postgres:postgres@localhost:5432/postgres pytest
```

## Trade-offs and what production would add

- **Lock timeout vs. long provider calls.** If the timeout is shorter than the
  provider's worst-case latency, a takeover can call the provider while the
  original call is still running. The provider-side key and the settlement
  fence keep that safe, but the timeout should still exceed the provider's
  client timeout.
- **Retention.** `purge_expired_keys` deletes finished keys after 24 h. After
  that, the same key creates a new payment, so the window must cover every
  client's retry horizon.
- **Background completion.** Here, a request stuck at `payment_created` is
  finished by the client's next retry. A production system also runs a
  *completer* job that drives abandoned keys to `finished`. Otherwise a client
  that never retries leaves a pending payment behind.
- **Reconciliation.** Settlement trusts the provider's answer. A daily
  reconciliation against the provider's reports catches anything that drifts.
- **Connection pooling.** With PgBouncer in transaction mode this design still
  works: it uses no session state and no advisory locks.

Pattern credit: Brandur Leach, [*Implementing Stripe-like Idempotency Keys in
Postgres*](https://brandur.org/idempotency-keys). This implementation is a
from-scratch version of what I have built for production fintech systems.

## License

MIT
