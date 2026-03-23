-- =====================================================
-- SENSOR CPU AVERAGE (FULL TABLE SCAN)
-- Demonstrates: Aggregation without indexes
-- =====================================================

SELECT sensor_id,
       avg(cpu_usage)
FROM sensor_metrics
GROUP BY sensor_id;


-- =====================================================
-- RECENT DATA FILTER (TIME RANGE SCAN)
-- Demonstrates: Sequential scan on timestamp filter
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE ts > now() - interval '2 months 20 days';


-- =====================================================
-- QUERY 3: SENSOR + TIME FILTER (COMPOSITE CONDITION)
-- Demonstrates: Multiple filters without index support
-- =====================================================

SELECT *
FROM sensor_metrics
WHERE sensor_id = 42
  AND ts > now() - interval '2 months 20 days';


-- =====================================================
-- QUERY 4: ORDER BY PERFORMANCE TEST
-- Demonstrates: Sorting large dataset without index
-- =====================================================

SELECT *
FROM sensor_metrics
ORDER BY ts DESC
LIMIT 1000;


-- =====================================================
-- QUERY 5: TIME BUCKET AGGREGATION
-- Demonstrates: Grouping overhead on raw time-series data
-- =====================================================

SELECT date_trunc('minute', ts) AS minute_bucket,
       avg(cpu_usage)
FROM sensor_metrics
GROUP BY minute_bucket
ORDER BY minute_bucket;
