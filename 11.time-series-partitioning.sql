-- =====================================================
-- SENSOR METRICS Reset and introduce partitioning
-- =====================================================
DROP TABLE IF EXISTS sensor_metrics;
-- =====================================================
-- SENSOR METRICS TABLE (RAW TIME-SERIES HEAP TABLE)
-- Demonstrates: Unoptimized append-only time-series storage
-- NO indexes, NO partitioning
-- =====================================================
CREATE TABLE sensor_metrics
(
    sensor_id   INT REFERENCES sensors (sensor_id),
    ts          TIMESTAMP,
    temperature DOUBLE PRECISION,
    cpu_usage   DOUBLE PRECISION,
    status      TEXT
) PARTITION BY RANGE (ts);

-- =====================================================
-- WEEKLY PARTITIONS (2026-01-01 → 2026-03-31)
-- Demonstrates: Fixed weekly partitioning strategy
-- =====================================================

DO
$$
    DECLARE
        start_date     DATE := DATE '2026-01-01';
        end_date       DATE := DATE '2026-04-01'; -- end boundary (exclusive)
        week_start     DATE;
        week_end       DATE;
        partition_name TEXT;
    BEGIN
        week_start := start_date;

        WHILE week_start < end_date
            LOOP
                week_end := week_start + INTERVAL '7 days';
                partition_name := format(
                        'sensor_metrics_%s_%s',
                        to_char(week_start, 'YYYYMMDD'),
                        to_char(week_end, 'YYYYMMDD')
                                  );

                EXECUTE format(
                        'CREATE TABLE %I PARTITION OF sensor_metrics FOR VALUES FROM (%L) TO (%L);',
                        partition_name,
                        week_start,
                        week_end
                        );

                week_start := week_end;
            END LOOP;
    END
$$;

-- =====================================================
-- DATA GENERATION (VARIABLE EVENT STREAM)
-- Demonstrates: Irregular time intervals (1ms - 1000ms)
-- Adjustable dataset size via parameter n
-- =====================================================
WITH RECURSIVE params AS (SELECT 10000000 AS n),
               base AS (SELECT i,
                               (random() * 1000)::int           AS ms,
                               (1 + floor(random() * 200))::int AS sensor_id
                        FROM generate_series(1, (SELECT n FROM params)) i),
               timeline AS (SELECT i,
                                   sensor_id,
                                   ms,
                                   SUM(ms) OVER (ORDER BY i) AS cum_ms
                            FROM base),
               final AS (SELECT sensor_id,
                                (TIMESTAMP '2026-01-01 00:00:00'
                                    + (cum_ms * INTERVAL '1 millisecond')) AS ts,
                                20 + random() * 15                         AS temperature,
                                random() * 100                             AS cpu_usage,
                                CASE
                                    WHEN random() > 0.97 THEN 'FAIL'
                                    ELSE 'OK'
                                    END                                    AS status
                         FROM timeline)
INSERT
INTO sensor_metrics (sensor_id, ts, temperature, cpu_usage, status)
SELECT sensor_id, ts, temperature, cpu_usage, status
FROM final;

-- =====================================================
-- BRIN INDEX ON TIME COLUMN
-- Demonstrates: Efficient storage-aware index for large
-- sequential time-series data
-- =====================================================

CREATE INDEX idx_sensor_metrics_ts_brin
    ON sensor_metrics
        USING BRIN (ts);


-- =====================================================
-- B-TREE INDEX FOR SENSOR + TIME LOOKUPS
-- Demonstrates: Fast point queries and range filtering
-- on high-selectivity dimensions
-- =====================================================

CREATE INDEX idx_sensor_metrics_sensor_ts
    ON sensor_metrics (sensor_id, ts DESC);


-- =====================================================
-- QUERY: CROSS-WEEK RANGE SCAN (MARCH)
-- Demonstrates: Multi-partition pruning
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE ts BETWEEN '2026-02-10' AND '2026-02-18';


-- =====================================================
-- QUERY: FULL MARCH DATA
-- Demonstrates: Multiple partitions scanned efficiently
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE ts >= '2026-02-01'
  AND ts < '2026-03-01';


-- =====================================================
-- QUERY: SENSOR ACTIVITY IN MARCH
-- Demonstrates: Composite filtering with partition pruning
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE sensor_id = 42
  AND ts >= '2026-03-01'
  AND ts < '2026-04-01';


-- =====================================================
-- QUERY: LATEST EVENTS
-- Demonstrates: Access mostly last partitions only
-- =====================================================

SELECT *
FROM sensor_metrics
ORDER BY ts DESC
LIMIT 1000;


-- =====================================================
-- QUERY: WEEKLY AGGREGATION
-- Demonstrates: Cross-partition aggregation cost
-- =====================================================

SELECT date_trunc('week', ts) AS week_bucket,
       avg(cpu_usage)
FROM sensor_metrics
GROUP BY week_bucket
ORDER BY week_bucket;

-- =====================================================
-- INDEXED QUERY: SENSOR + TIME RANGE LOOKUP
-- Demonstrates: Uses composite B-tree index
-- (sensor_id, ts) for fast pruning + lookup
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE sensor_id = 42
  AND ts >= '2026-02-01'
  AND ts < '2026-02-08';


-- =====================================================
-- INDEXED QUERY: LATEST SENSOR READINGS
-- Demonstrates: Index-backed ORDER BY + LIMIT (top-N)
-- Avoids full sort using (sensor_id, ts DESC) index
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE sensor_id = 42
ORDER BY ts DESC
LIMIT 50;


-- =====================================================
-- INDEXED QUERY: HOT SENSOR DETECTION
-- Demonstrates: Selective filter benefits from B-tree
-- + partition pruning on ts
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE sensor_id = 17
  AND status = 'FAIL'
  AND ts >= now() - interval '2 months 20 days';


-- =====================================================
-- INDEXED QUERY: RECENT SYSTEM FAILURES (ALL SENSORS)
-- Demonstrates: BRIN index on ts enables efficient
-- scanning of large time-range without full table scan
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE status = 'FAIL'
  AND ts >= now() - interval '2 months 20 days';


-- =====================================================
-- INDEXED QUERY: SENSOR ACTIVITY DISTRIBUTION
-- Demonstrates: Index-assisted aggregation pre-filtering
-- (reduces scanned rows via sensor_id index)
-- =====================================================

SELECT sensor_id,
       count(*)       AS events,
       avg(cpu_usage) AS avg_cpu
FROM sensor_metrics
WHERE sensor_id BETWEEN 1 AND 50
  AND ts >= '2026-02-01'
  AND ts < '2026-03-01'
GROUP BY sensor_id;


-- =====================================================
-- INDEXED QUERY: STATUS BREAKDOWN FOR A SENSOR
-- Demonstrates: Filtered scan using composite index
-- =====================================================

SELECT status,
       count(*)
FROM sensor_metrics
WHERE sensor_id = 10
  AND ts >= '2026-02-01'
  AND ts < '2026-03-01'
GROUP BY status;
