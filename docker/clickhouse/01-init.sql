-- silver: loaded from the Delta silver layer; marts: dbt models; ops: pipeline runs and drift.
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS marts;
CREATE DATABASE IF NOT EXISTS ops;

-- Phase 0 fixture so dbt has a source to model; Phase 2's loader owns the real silver DDL.
CREATE TABLE IF NOT EXISTS silver.page_view
(
    event_id String,
    event_time DateTime,
    event_date Date,
    site_id String,
    user_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (site_id, event_time);
