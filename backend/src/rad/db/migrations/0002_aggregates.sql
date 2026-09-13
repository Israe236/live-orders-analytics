-- Time-bucketed aggregates. They are maintained incrementally by the batch writer, inside the
-- same SQL statement that inserts the raw events (see rad/processing/writer.py), so they are
-- always exactly consistent with the events table. Dashboards read only these tables.

-- An order's status only moves "forward" by this rank. Events that arrive out of order
-- (e.g. `paid` before `placed`) therefore still produce the right final status.
CREATE FUNCTION order_status_rank(status text) RETURNS int
    LANGUAGE sql IMMUTABLE PARALLEL SAFE
    RETURN CASE status
        WHEN 'placed' THEN 1
        WHEN 'paid' THEN 2
        WHEN 'shipped' THEN 3
        WHEN 'cancelled' THEN 4
    END;

-- One row per minute of event time.
CREATE TABLE agg_minute (
    bucket            timestamptz   PRIMARY KEY,
    placed_count      bigint        NOT NULL DEFAULT 0,
    paid_count        bigint        NOT NULL DEFAULT 0,
    shipped_count     bigint        NOT NULL DEFAULT 0,
    cancelled_count   bigint        NOT NULL DEFAULT 0,
    placed_value_mad  numeric(18,2) NOT NULL DEFAULT 0,  -- value of orders placed
    revenue_mad       numeric(18,2) NOT NULL DEFAULT 0   -- value of orders paid
);

-- One row per (dimension, minute, value), e.g. ('city', 12:04, 'Rabat').
-- A single table for all breakdowns keeps the write statement short; the primary key starts
-- with `dimension` because every read filters on one dimension and a time range.
CREATE TABLE agg_minute_dimension (
    dimension         text          NOT NULL CHECK (dimension IN ('category', 'city', 'payment_method')),
    bucket            timestamptz   NOT NULL,
    value             text          NOT NULL,
    placed_count      bigint        NOT NULL DEFAULT 0,
    paid_count        bigint        NOT NULL DEFAULT 0,
    cancelled_count   bigint        NOT NULL DEFAULT 0,
    revenue_mad       numeric(18,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (dimension, bucket, value)
);

-- Current state of every order (one row per order, updated as its events arrive).
CREATE TABLE orders (
    order_id        uuid          PRIMARY KEY,
    status          text          NOT NULL CHECK (order_status_rank(status) IS NOT NULL),
    amount_mad      numeric(12,2) NOT NULL,
    category        text          NOT NULL,
    city            text          NOT NULL,
    payment_method  text          NOT NULL,
    first_event_at  timestamptz   NOT NULL,
    last_event_at   timestamptz   NOT NULL
);

-- Counter per status, adjusted by +1/-1 when orders change status. Reading "orders per
-- status" is then 4 rows instead of a GROUP BY over every order.
CREATE TABLE order_status_counts (
    status       text   PRIMARY KEY,
    order_count  bigint NOT NULL
);

-- Processing-time statistics: bucketed by when data was *received*, not when it happened.
-- Used for throughput and the dead-letter-ratio alert.
CREATE TABLE ingest_minute (
    bucket             timestamptz PRIMARY KEY,
    accepted_count     bigint      NOT NULL DEFAULT 0,
    duplicate_count    bigint      NOT NULL DEFAULT 0,
    dead_letter_count  bigint      NOT NULL DEFAULT 0
);
