-- =====================================================
-- CREATE SCHEMA AND QUEUE TABLE
-- Demonstrates: Messaging layer foundation
-- =====================================================

CREATE SCHEMA IF NOT EXISTS mq;

DROP TABLE IF EXISTS mq.queue;

CREATE TABLE mq.queue
(
    id         BIGSERIAL PRIMARY KEY,
    event_type TEXT  NOT NULL,
    payload    JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT now(),
    processed  BOOLEAN   DEFAULT FALSE
);


-- =====================================================
-- GENERATE RANDOM EVENTS (BATCH PRODUCER)
-- Demonstrates: Simulating a real event stream
-- =====================================================

INSERT INTO mq.queue (event_type, payload)
SELECT (ARRAY ['sensor_reading', 'sensor_failure', 'sensor_recovery'])
           [1 + floor(random() * 3)],

       jsonb_build_object(
               'sensor_id', (1 + floor(random() * 200))::int,
               'cpu', round((random() * 100)::numeric, 2),
               'temperature', round((20 + random() * 15)::numeric, 2),
               'status',
               CASE
                   WHEN random() > 0.95 THEN 'FAIL'
                   ELSE 'OK'
                   END,
               'ts', now() - (random() * interval '1 hour')
       )
FROM generate_series(1, 50);
-- CHANGE VOLUME HERE


-- =====================================================
-- BASIC QUEUE CONSUMPTION
-- Demonstrates: Pull-based message retrieval
-- =====================================================

SELECT *
FROM mq.queue
WHERE processed = FALSE
ORDER BY created_at
LIMIT 10;


-- =====================================================
-- SAFE CONCURRENT CONSUMPTION
-- Demonstrates: Multi-consumer processing
-- =====================================================

WITH cte AS (SELECT id
             FROM mq.queue
             WHERE processed = FALSE
             ORDER BY created_at
                 FOR UPDATE SKIP LOCKED
             LIMIT 10)
UPDATE mq.queue
SET processed = TRUE
WHERE id IN (SELECT id FROM cte)
RETURNING *;


-- =====================================================
-- LISTEN FOR EVENTS (RUN IN SEPARATE SESSION)
-- =====================================================

LISTEN mq_channel;


-- =====================================================
-- TRIGGER FUNCTION WITH DYNAMIC PAYLOAD
-- Demonstrates: Real-time event notification
-- =====================================================

CREATE OR REPLACE FUNCTION mq.notify_queue_event()
    RETURNS trigger AS
$$
BEGIN
    PERFORM pg_notify(
            'mq_channel',
            json_build_object(
                    'id', NEW.id,
                    'event_type', NEW.event_type
            )::text
            );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- =====================================================
-- TRIGGER ON INSERT
-- =====================================================

DROP TRIGGER IF EXISTS trg_notify_queue ON mq.queue;

CREATE TRIGGER trg_notify_queue
    AFTER INSERT
    ON mq.queue
    FOR EACH ROW
EXECUTE FUNCTION mq.notify_queue_event();


-- =====================================================
-- INSERT RANDOM EVENTS (REAL-TIME STREAM)
-- Demonstrates: Continuous event generation
-- =====================================================

INSERT INTO mq.queue (event_type, payload)
SELECT (ARRAY ['sensor_reading', 'sensor_failure', 'sensor_recovery'])
           [1 + floor(random() * 3)],

       jsonb_build_object(
               'sensor_id', (1 + floor(random() * 200))::int,
               'cpu', round((random() * 100)::numeric, 2),
               'temperature', round((20 + random() * 15)::numeric, 2),
               'status',
               CASE
                   WHEN random() > 0.97 THEN 'FAIL'
                   ELSE 'OK'
                   END,
               'ts', now()
       )
FROM generate_series(1, 10);
