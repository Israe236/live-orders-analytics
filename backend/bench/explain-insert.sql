-- EXPLAIN ANALYZE of the writer's insert statement on a realistic 2,000-event batch.
-- The same statement as rad/processing/writer.py, with the unnest($n) input replaced by
-- generated rows. Run twice inside transactions that are rolled back: nothing is kept.
\pset pager off
\timing on

BEGIN;
EXPLAIN (ANALYZE, BUFFERS, TIMING, SUMMARY, COSTS OFF)
WITH input AS (
    SELECT uuidv7() AS event_id,
           uuidv7() AS order_id,
           (ARRAY['order_placed','order_paid','order_shipped','order_cancelled'])[1 + floor(random() * 4)::int] AS event_type,
           now() AS occurred_at,
           round((50 + random() * 4950)::numeric, 2) AS amount_mad,
           (ARRAY['electronics','fashion','home','beauty','grocery','sports','books','toys'])[1 + floor(random() * 8)::int] AS category,
           (ARRAY['Casablanca','Rabat','Marrakech','Fes','Tangier','Agadir','Meknes','Oujda','Kenitra','Tetouan'])[1 + floor(random() * 10)::int] AS city,
           (ARRAY['card','cash_on_delivery','wallet','bank_transfer'])[1 + floor(random() * 4)::int] AS payment_method
    FROM generate_series(1, 2000)
),
inserted AS (
    INSERT INTO events (
        event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method
    )
    SELECT event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method
    FROM input
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id, order_id, occurred_at, amount_mad, category, city, payment_method,
              date_trunc('minute', occurred_at) AS bucket,
              replace(event_type, 'order_', '') AS status
),
minute_totals AS (
    INSERT INTO agg_minute AS a (
        bucket, placed_count, paid_count, shipped_count, cancelled_count,
        placed_value_mad, revenue_mad
    )
    SELECT bucket,
           count(*) FILTER (WHERE status = 'placed'),
           count(*) FILTER (WHERE status = 'paid'),
           count(*) FILTER (WHERE status = 'shipped'),
           count(*) FILTER (WHERE status = 'cancelled'),
           coalesce(sum(amount_mad) FILTER (WHERE status = 'placed'), 0),
           coalesce(sum(amount_mad) FILTER (WHERE status = 'paid'), 0)
    FROM inserted
    GROUP BY bucket
    ON CONFLICT (bucket) DO UPDATE SET
        placed_count     = a.placed_count     + excluded.placed_count,
        paid_count       = a.paid_count       + excluded.paid_count,
        shipped_count    = a.shipped_count    + excluded.shipped_count,
        cancelled_count  = a.cancelled_count  + excluded.cancelled_count,
        placed_value_mad = a.placed_value_mad + excluded.placed_value_mad,
        revenue_mad      = a.revenue_mad      + excluded.revenue_mad
),
dimension_totals AS (
    INSERT INTO agg_minute_dimension AS d (
        dimension, bucket, value, placed_count, paid_count, cancelled_count, revenue_mad
    )
    SELECT dim.dimension, i.bucket, dim.value,
           count(*) FILTER (WHERE i.status = 'placed'),
           count(*) FILTER (WHERE i.status = 'paid'),
           count(*) FILTER (WHERE i.status = 'cancelled'),
           coalesce(sum(i.amount_mad) FILTER (WHERE i.status = 'paid'), 0)
    FROM inserted AS i
    CROSS JOIN LATERAL (
        VALUES ('category', i.category), ('city', i.city), ('payment_method', i.payment_method)
    ) AS dim(dimension, value)
    WHERE i.status <> 'shipped'
    GROUP BY dim.dimension, i.bucket, dim.value
    ON CONFLICT (dimension, bucket, value) DO UPDATE SET
        placed_count    = d.placed_count    + excluded.placed_count,
        paid_count      = d.paid_count      + excluded.paid_count,
        cancelled_count = d.cancelled_count + excluded.cancelled_count,
        revenue_mad     = d.revenue_mad     + excluded.revenue_mad
),
order_updates AS (
    INSERT INTO orders AS o (
        order_id, status, amount_mad, category, city, payment_method,
        first_event_at, last_event_at
    )
    SELECT DISTINCT ON (order_id)
           order_id, status, amount_mad, category, city, payment_method,
           min(occurred_at) OVER (PARTITION BY order_id),
           max(occurred_at) OVER (PARTITION BY order_id)
    FROM inserted
    ORDER BY order_id, order_status_rank(status) DESC
    ON CONFLICT (order_id) DO UPDATE SET
        status = CASE
                     WHEN order_status_rank(excluded.status) > order_status_rank(o.status)
                     THEN excluded.status
                     ELSE o.status
                 END,
        first_event_at = least(o.first_event_at, excluded.first_event_at),
        last_event_at  = greatest(o.last_event_at, excluded.last_event_at)
    RETURNING old.status AS old_status, new.status AS new_status
),
status_counts AS (
    INSERT INTO order_status_counts AS s (status, order_count)
    SELECT status, sum(delta)
    FROM (
        SELECT new_status AS status, 1 AS delta
        FROM order_updates
        WHERE old_status IS DISTINCT FROM new_status
        UNION ALL
        SELECT old_status, -1
        FROM order_updates
        WHERE old_status IS NOT NULL AND old_status <> new_status
    ) AS changes
    GROUP BY status
    ON CONFLICT (status) DO UPDATE SET order_count = s.order_count + excluded.order_count
),
ingest_totals AS (
    INSERT INTO ingest_minute AS m (bucket, accepted_count, duplicate_count)
    SELECT date_trunc('minute', now()),
           (SELECT count(*) FROM inserted),
           (SELECT count(*) FROM input) - (SELECT count(*) FROM inserted)
    ON CONFLICT (bucket) DO UPDATE SET
        accepted_count  = m.accepted_count  + excluded.accepted_count,
        duplicate_count = m.duplicate_count + excluded.duplicate_count
)
SELECT count(*) FROM inserted;
ROLLBACK;

\echo '==== second run (warm) ===='
BEGIN;
EXPLAIN (ANALYZE, TIMING, SUMMARY, COSTS OFF)
WITH input AS (
    SELECT uuidv7() AS event_id, uuidv7() AS order_id,
           (ARRAY['order_placed','order_paid','order_shipped','order_cancelled'])[1 + floor(random() * 4)::int] AS event_type,
           now() AS occurred_at, round((50 + random() * 4950)::numeric, 2) AS amount_mad,
           (ARRAY['electronics','fashion','home','beauty','grocery','sports','books','toys'])[1 + floor(random() * 8)::int] AS category,
           (ARRAY['Casablanca','Rabat','Marrakech','Fes','Tangier','Agadir','Meknes','Oujda','Kenitra','Tetouan'])[1 + floor(random() * 10)::int] AS city,
           (ARRAY['card','cash_on_delivery','wallet','bank_transfer'])[1 + floor(random() * 4)::int] AS payment_method
    FROM generate_series(1, 2000)
),
inserted AS (
    INSERT INTO events (event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method)
    SELECT event_id, order_id, event_type, occurred_at, amount_mad, category, city, payment_method FROM input
    ON CONFLICT (event_id) DO NOTHING
    RETURNING order_id, occurred_at, amount_mad, category, city, payment_method,
              date_trunc('minute', occurred_at) AS bucket, replace(event_type, 'order_', '') AS status
),
order_updates AS (
    INSERT INTO orders AS o (order_id, status, amount_mad, category, city, payment_method, first_event_at, last_event_at)
    SELECT DISTINCT ON (order_id) order_id, status, amount_mad, category, city, payment_method,
           min(occurred_at) OVER (PARTITION BY order_id), max(occurred_at) OVER (PARTITION BY order_id)
    FROM inserted ORDER BY order_id, order_status_rank(status) DESC
    ON CONFLICT (order_id) DO UPDATE SET status = o.status
    RETURNING old.status AS old_status, new.status AS new_status
)
SELECT count(*) FROM order_updates;
ROLLBACK;
