-- Raw event log. Append-only. The dashboard never scans this table: it reads the small
-- aggregate tables instead (see later migrations).
CREATE TABLE events (
    -- The producer's event id doubles as the idempotency key: a retried event is ignored.
    event_id        uuid          PRIMARY KEY,
    order_id        uuid          NOT NULL,
    event_type      text          NOT NULL CHECK (event_type IN
                        ('order_placed', 'order_paid', 'order_shipped', 'order_cancelled')),
    occurred_at     timestamptz   NOT NULL,
    received_at     timestamptz   NOT NULL DEFAULT now(),
    amount_mad      numeric(12,2) NOT NULL CHECK (amount_mad > 0),
    category        text          NOT NULL,
    city            text          NOT NULL,
    payment_method  text          NOT NULL
);

-- BRIN index: a few kilobytes for millions of rows, almost free to maintain on insert.
-- Works because rows arrive roughly in time order. Only used for occasional time-range
-- queries on raw data (audits, backfills, test verification) — never on the hot path.
CREATE INDEX events_occurred_at_brin ON events USING brin (occurred_at);

-- Everything that could not be ingested, kept with the reason so nothing is silently lost.
CREATE TABLE dead_letter_events (
    id            bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    received_at   timestamptz NOT NULL DEFAULT now(),
    reason        text        NOT NULL,
    error_detail  jsonb,
    raw_payload   text        NOT NULL
);

CREATE INDEX dead_letter_events_received_at_brin ON dead_letter_events USING brin (received_at);
