-- Retention deletes orders by the time they were first seen. That column never changes after the
-- insert and follows insertion order, so a tiny BRIN index is enough to find old orders.
CREATE INDEX orders_first_event_at_brin ON orders USING brin (first_event_at);
