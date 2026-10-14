-- One row per (client, Idempotency-Key). It is both the lock that serialises
-- retries of the same request and the record of how far the request got.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    id              bigserial PRIMARY KEY,
    client_id       text        NOT NULL,
    key             text        NOT NULL,
    request_hash    text        NOT NULL,
    -- started -> payment_created -> finished
    recovery_point  text        NOT NULL DEFAULT 'started',
    locked_at       timestamptz,
    response_code   integer,
    response_body   jsonb,
    created_at      timestamptz NOT NULL DEFAULT now(),
    last_run_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (client_id, key)
);

CREATE TABLE IF NOT EXISTS payments (
    id                  uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- SET NULL so expired keys can be purged without touching payments.
    idempotency_key_id  bigint      UNIQUE REFERENCES idempotency_keys (id) ON DELETE SET NULL,
    client_id           text        NOT NULL,
    amount_minor        bigint      NOT NULL CHECK (amount_minor > 0),
    currency            char(3)     NOT NULL,
    source_account      text        NOT NULL,
    destination_account text        NOT NULL,
    -- pending -> succeeded | failed
    status              text        NOT NULL DEFAULT 'pending',
    provider_charge_id  text,
    failure_reason      text,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- Double-entry ledger: every succeeded payment writes two rows that sum to zero.
CREATE TABLE IF NOT EXISTS ledger_entries (
    id           bigserial   PRIMARY KEY,
    payment_id   uuid        NOT NULL REFERENCES payments (id),
    account      text        NOT NULL,
    amount_minor bigint      NOT NULL,
    currency     char(3)     NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now()
);

-- State of the simulated external provider. In real life this lives on the
-- provider's side; here it lets tests count how many charges really happened.
CREATE TABLE IF NOT EXISTS provider_charges (
    id              text        PRIMARY KEY DEFAULT 'ch_' || replace(gen_random_uuid()::text, '-', ''),
    idempotency_key text        NOT NULL UNIQUE,
    amount_minor    bigint      NOT NULL,
    currency        char(3)     NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now()
);
