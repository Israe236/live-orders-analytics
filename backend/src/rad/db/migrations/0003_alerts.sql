-- Alert history. One row per alert: inserted when it fires, updated when it resolves.
CREATE TABLE alerts (
    id           uuid             PRIMARY KEY,
    rule         text             NOT NULL,
    severity     text             NOT NULL CHECK (severity IN ('warning', 'critical')),
    status       text             NOT NULL CHECK (status IN ('firing', 'resolved')),
    message      text             NOT NULL,
    value        double precision,
    threshold    double precision NOT NULL,
    started_at   timestamptz      NOT NULL,
    resolved_at  timestamptz
);

-- The UI lists the most recent alerts first.
CREATE INDEX alerts_started_at_idx ON alerts (started_at DESC);
