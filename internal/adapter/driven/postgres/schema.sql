-- Schema for the order book's persistent state.
--
-- Applied idempotently at startup (see Migrate), so `docker compose up` needs
-- no separate migration step. Everything is integer/BIGINT to match the
-- domain's exact-integer money model (no floating point).

CREATE TABLE IF NOT EXISTS wallets (
    user_id       TEXT PRIMARY KEY,
    cop_available BIGINT NOT NULL DEFAULT 0 CHECK (cop_available >= 0),
    cop_locked    BIGINT NOT NULL DEFAULT 0 CHECK (cop_locked    >= 0),
    vib_available BIGINT NOT NULL DEFAULT 0 CHECK (vib_available >= 0),
    vib_locked    BIGINT NOT NULL DEFAULT 0 CHECK (vib_locked    >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Append-only trade history: the traceability backbone.
CREATE TABLE IF NOT EXISTS trades (
    id            TEXT PRIMARY KEY,
    symbol        TEXT   NOT NULL,
    price         BIGINT NOT NULL,
    quantity      BIGINT NOT NULL,
    buy_order_id  TEXT   NOT NULL,
    sell_order_id TEXT   NOT NULL,
    buyer_id      TEXT   NOT NULL,
    seller_id     TEXT   NOT NULL,
    sequence      BIGINT NOT NULL,
    executed_at   TIMESTAMPTZ NOT NULL
);

-- Newest-first listing and the sequence scan both benefit from this index.
CREATE INDEX IF NOT EXISTS trades_sequence_desc_idx ON trades (sequence DESC);
CREATE INDEX IF NOT EXISTS trades_buyer_idx        ON trades (buyer_id);
CREATE INDEX IF NOT EXISTS trades_seller_idx       ON trades (seller_id);

-- Order status projection, queried by clients instead of the hot in-memory book.
CREATE TABLE IF NOT EXISTS orders (
    id              TEXT PRIMARY KEY,
    user_id         TEXT   NOT NULL,
    symbol          TEXT   NOT NULL,
    side            TEXT   NOT NULL,
    price           BIGINT NOT NULL,
    quantity        BIGINT NOT NULL,
    status          TEXT   NOT NULL,
    filled_quantity BIGINT NOT NULL,
    sequence        BIGINT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS orders_user_idx ON orders (user_id);

-- Settlement idempotency journal. An at-least-once log (Kafka/Redpanda) can
-- redeliver an event; recording its ID here makes money movement
-- effectively-once. The PRIMARY KEY is what does the actual deduplication.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
