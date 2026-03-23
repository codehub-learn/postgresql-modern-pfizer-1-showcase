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
-- Run again the queries in 09.time-series-indexes.sql
-- =====================================================
