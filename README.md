# Sightline

Event analytics warehouse with an Angular dashboard: product and ad events stream through Kafka, land as Delta tables on MinIO by Spark Structured Streaming, are shaped into silver tables by Spark batch jobs, served from ClickHouse, modeled and tested with dbt, orchestrated by Airflow with a data-quality gate and clean backfills, and read by an Angular dashboard. A scikit-learn model trained from the warehouse, tracked in MLflow and exported to ONNX, is handed to the companion project Gatekeeper, and a weekly drift check watches its features.

Every event is synthetic and seeded, so every number in this README is reproducible from one command. Every pipeline step is idempotent, so a day can be rerun or backfilled without duplicating a row. Everything runs in Docker Compose with no cloud account and no card on file; one Spark job is mirrored in Databricks Free Edition so the word is backed by a run.

**Status.** Design complete, build not started. This README describes the target system; the phase list at the end records what has landed.

## Ten-minute tour

The intended path once Phase 6 lands:

```bash
docker compose up -d --wait                      # Redpanda 19092 + registry 18081, MinIO 9002, ClickHouse 8123, Jaeger, Grafana 3001
scripts/register-schemas.sh                      # seven JSON Schemas, BACKWARD compatibility
uv run python -m pipeline.produce --days 3 --rate 2000 --seed 42   # diurnal traffic, heavy sites, three planted anomalies
docker compose logs -f spark-stream | grep landed # bronze Delta partitions appear on MinIO
docker compose --profile orchestrate up -d       # Airflow 8082
airflow dags backfill sightline_daily -s 2026-11-01 -e 2026-11-03
clickhouse-client -q "SELECT event_date, sum(revenue) FROM marts.agg_daily_site GROUP BY 1 ORDER BY 1"
cd warehouse && dbt test                         # the contract between layers
open http://localhost:4200                       # Angular dashboard: revenue, funnel, sites, anomalies, pipeline health
open http://localhost:5001                       # MLflow: gatekeeper-risk runs, metrics, the registered model
```

## Architecture

```
pipeline.produce (seeded synthetic events, OpenRTB-shaped bids) ──► Redpanda topics + schema registry
                                                                         │
                                            Spark Structured Streaming (land_bronze, checkpoints, idempotent MERGE)
                                                                         ▼
MinIO  s3a://lake/  bronze/<event>/event_date=…  (Delta)   ──► build_silver (Spark batch, replaceWhere) ──► silver/ (Delta)
                                                                         │
                                                       load_clickhouse (INSERT … SELECT FROM s3/delta)
                                                                         ▼
                                   ClickHouse  (MergeTree silver tables) ──► dbt (staging, dims, facts, aggregates, tests, docs)
                                                                         │
                 Airflow sightline_daily: sensor ─ silver ─ load ─ dbt build (quality gate) ─ quality_report ─ retrain_check
                                                                         │
                               FastAPI read API (:8002) ◄───────────────┘            MLflow (:5001) ── skl2onnx ──► Gatekeeper
                                      │                                                        ▲
                               Angular dashboard (:4200)                              features.py over silver, weekly PSI drift
```

| Directory | Stack | Role | Port |
|---|---|---|---|
| `pipeline/` | Python, PySpark, Delta Lake, confluent-kafka, scikit-learn, MLflow | Producer, streaming and batch jobs, ClickHouse loader, features, training, Scrapy spider | – |
| `warehouse/` | dbt Core, dbt-clickhouse, sqlfluff | Staging, dimensions, facts, aggregates, tests, docs, snapshots | – |
| `airflow/` | Airflow 3 | `sightline_daily` DAG | 8082 |
| `api/` | FastAPI, clickhouse-connect | Read API for the dashboard | 8002 |
| `dashboard/` | Angular, Angular Material, Sass, ECharts | Overview, revenue, funnel, sites, anomalies, pipeline health | 4200 |
| `notebooks/` | Databricks Free Edition | Mirror of the silver job on a sample | – |

## Stack

Python 3.13 · PySpark · Spark Structured Streaming · Delta Lake · Parquet · Redpanda (Kafka API, schema registry) · MinIO (S3 API) · ClickHouse · dbt Core · Apache Airflow 3 · FastAPI · scikit-learn · MLflow · ONNX · Scrapy · Angular · TypeScript · Angular Material · Sass · ECharts · Cypress · Lighthouse CI · Databricks Free Edition · Grafana · OpenTelemetry · GitHub Actions

Prerequisites: Docker with Compose v2, uv, Java 21 (Spark driver), Node 26. Exact versions are pinned when Phase 0 lands.

## Events and ingestion

Seven event types with JSON Schemas registered in Redpanda's schema registry under `BACKWARD` compatibility, so a breaking producer change is refused before any consumer sees it: `page_view`, `signup`, `bid_request` and `bid_response` (an OpenRTB-shaped subset with `imp[]`, `site`, `device`, `user`, `bidfloor`, `price`, `adomain`), `impression`, `click`, `conversion`, plus `gatekeeper_decision` imported from Gatekeeper's contract.

`pipeline.produce` is a seeded generator with a realistic shape: a diurnal curve, a few heavy sites, a long tail of users, and three planted anomalies the dashboard and the model are expected to find (a bot ASN, a price spike on one site, a conversion fraud ring). Fixed seed, fixed output.

**Bronze.** `land_bronze` is a Spark Structured Streaming job reading every topic, validating each record against its schema, and writing Delta tables partitioned by `event_date` to `s3a://lake/bronze/<event>` on MinIO. Checkpoints live on MinIO; each micro-batch is written idempotently (a `MERGE` on event id, or Delta's transactional idempotent writes; the choice is recorded in the roadmap when made), so a job restart mid-stream replays without a duplicate. Records that fail validation land in `bronze/_rejected` with the error, never silently dropped. The test for this is the one that matters: produce a day, kill the job in the middle, restart, count.

## Warehouse

**Silver.** `build_silver` runs per `event_date`: deduplication, type coercion, sessionization at thirty minutes of inactivity, user-agent and bot flags, bids joined to impressions and clicks, currency-normalized prices. Each run replaces exactly its own partition (`replaceWhere`), so rerunning a day is safe by construction.

**ClickHouse.** Silver partitions are loaded into `MergeTree` tables ordered by `(site_id, event_time)`, partitioned by month, with a thirteen-month TTL, by dropping and reloading the day's partition. ClickHouse is the warehouse the dashboard reads: hundreds of millions of synthetic rows aggregate in well under a second on a laptop, which is the property an analytics product needs and Postgres does not have at that size.

**dbt.** Staging views over the loaded silver tables, dimensional marts (`dim_site`, `dim_user`, `dim_campaign` as a type-2 snapshot, `fct_sessions`, `fct_bids`, `fct_conversions`) and aggregate tables (`agg_daily_site`, `agg_hourly_revenue` on `AggregatingMergeTree`, `agg_funnel`). Generic tests (`unique`, `not_null`, `accepted_values`, relationships) plus custom ones that state business truths: revenue equals the sum of winning bids, the funnel is monotonic. `dbt docs` is published to GitHub Pages with lineage from staging to aggregates. The dbt tests are the contract between layers, and the Airflow DAG treats a failing test as a reason to stop.

## Orchestration

`sightline_daily` is one Airflow DAG with `catchup=True` and `max_active_runs=1`: a sensor waits for the day's bronze partition, then silver, then the ClickHouse load, then `dbt build` as the quality gate, then a `quality_report` task writing row counts, freshness and the test summary to `ops.pipeline_runs`, and a weekly `retrain_check` branch. Retries with backoff, an SLA, and task-level idempotency documented per task. `airflow dags backfill` over a date range reproduces the original counts exactly because every step replaces rather than appends. A planted bad partition fails the gate and leaves that day's marts unchanged.

## Read API and dashboard

A thin FastAPI service exposes `/metrics/daily`, `/metrics/hourly`, `/funnel`, `/sites/{id}` and `/anomalies` as parameterized ClickHouse queries with a sixty-second cache, and publishes OpenAPI. Authentication is a static read token on purpose; this is internal analytics, and authentication is proven elsewhere.

The dashboard is Angular with standalone components and signals, Angular Material themed from Sass design tokens, ECharts for time series, funnel and heatmap, a client generated from the OpenAPI document, lazy-loaded routes, and layouts tested at 360, 768 and 1280 pixels. Pages: overview, revenue (hourly series, bid win rate), funnel, sites (sortable, virtual scroll), anomalies (the three planted ones are visible from the seed), pipeline health (from `ops.pipeline_runs` and `ops.model_drift`). Cypress drives the flows against the compose stack, `cypress-axe` checks WCAG 2.2 AA on every page, and Lighthouse CI enforces budgets (performance 90, accessibility 95).

## Machine learning

Features are extracted from silver by `pipeline/features.py` according to `docs/FEATURES.md`, the same specification Gatekeeper implements in TypeScript; a shared JSON fixture asserts both produce the same vector for the same event. Labels come from the planted abuse patterns. The model is a scikit-learn `LogisticRegression` in a `Pipeline` with scaling and balanced class weights, evaluated on ROC AUC, precision at the operating threshold and calibration, with every run tracked in MLflow (SQLite backend, MinIO artifacts) and the best run registered as `gatekeeper-risk`. `skl2onnx` exports it, a test asserts the ONNX output matches scikit-learn within 1e-6, and `scripts/handoff-model.sh` copies the file and its model card into Gatekeeper. Each week the DAG computes the population stability index per feature against the training distribution and writes it to `ops.model_drift`; the dashboard shows it.

Logistic regression is the right size for this problem: synthetic labels, a dozen features, a consumer that needs a score in a few milliseconds from Node. A gradient-boosted model would score higher on the synthetic data and prove nothing more.

## Databricks and scraping

`notebooks/build_silver_databricks.py` is the silver logic run once on Databricks Free Edition against a one-day Parquet sample uploaded by the CLI, with a screenshot in this README. It is a mirror of the local job and is described as nothing more. `pipeline/scrape/` holds one Scrapy spider that reads a public, scrape-permitted index into a `reference_sites` dimension, obeying `robots.txt` with a polite delay, tested against a recorded HTML fixture.

## Design decisions

- **Spark Structured Streaming for landing, not Kafka Connect.** The streaming job validates, routes rejects and writes idempotently with one engine that also runs the batch layer. Connect is a stretch item with a written comparison.
- **Delta on MinIO as the lake, ClickHouse as the warehouse.** Delta gives transactional, idempotent partition replacement for the pipeline; ClickHouse gives sub-second aggregates for the dashboard. One system for both would do one job badly.
- **Gold in dbt, not in Spark.** Marts are SQL with tests and docs that an analyst can read and change; keeping them in dbt makes the warehouse the contract and Spark the plumbing.
- **Idempotency by replacement.** `MERGE` into bronze, `replaceWhere` for silver, drop-and-reload for ClickHouse partitions. Backfill is then just rerunning, and the test suite proves it.
- **dbt tests as the quality gate.** The DAG stops on a failed test and marks the day. Great Expectations was considered and set aside; the dbt tests plus a small custom check say the same thing with less machinery.
- **Angular, not React or Vue.** Both are proven elsewhere; the dashboard exists to prove Angular with the same care, including accessibility and performance budgets.
- **ONNX for the hand-off.** The consumer is a Node service; ONNX is the format that runs there without a Python sidecar.
- **Synthetic data only.** No real users, no licensing risk, and the planted anomalies give the dashboard and the model a ground truth to be measured against.
- **Zero cost.** Compose for every service, Databricks Free Edition, GitHub Pages and Actions. Snowflake and BigQuery stay out unless a trial is confirmed card-free.

## Tests

Planned, enforced in CI from Phase 0:

| Level | Where | What |
|---|---|---|
| Unit | `pipeline/` | generator properties under a fixed seed, feature extraction against the shared fixture, ONNX equals scikit-learn |
| Spark (local mode) | `pipeline/jobs` | bronze replay yields the same count, rejects land with their error, silver reruns are idempotent, late events land in the right partition |
| Registry | `pipeline/contracts` | a breaking schema change is refused by a real registry container |
| Warehouse | `warehouse/` | `dbt build` on a one-day fixture, every generic and custom test |
| DAG | `airflow/` | import and structure tests, one `airflow dags test` run, backfill reproduces counts |
| API | `api/` | each endpoint against a fixture ClickHouse |
| End to end | `dashboard/` | Cypress flows, `cypress-axe`, Lighthouse budgets |

## Phases

Each phase is a branch merged by pull request and tracked as a GitHub issue.

- [ ] 0 Skeleton, compose, CI
- [ ] 1 Event contracts, producer, Spark streaming to bronze Delta
- [ ] 2 Silver and gold: Spark batch, ClickHouse serving tables, dbt models and tests
- [ ] 3 Airflow: DAG, sensors, data-quality gate, backfill
- [ ] 4 Read API and Angular dashboard
- [ ] 5 ML: features, scikit-learn, MLflow, ONNX hand-off to Gatekeeper, drift
- [ ] 6 Databricks mirror, Scrapy source, observability, demo, README polish
- [ ] 7 Stretch: Iceberg, Trino, Looker Studio, Kafka Connect, Snowflake (if card-free), Great Expectations

## Out of scope, on purpose

Real user data, a cloud deployment, a multi-node Spark cluster, real-time bidding itself (the events are bid-shaped; nothing bids), and a licensed BI tool. Each would be a layer on top; none changes the pipeline design shown here.

## License

Copyright (C) 2026 James Knox Polk

This program is free software: you can redistribute it and/or modify it under the terms of the GNU Affero General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version. See [LICENSE](LICENSE).
