# ELT Retail Pipeline

Local ELT pipeline for retail analytics, orchestrated with Airflow:

```text
MySQL (retailerdb + supplierdb)
  -> retail_ingestion DAG
  -> GCS landing (Parquet)
  -> retail_transform DAG
  -> DuckDB bronze -> silver -> gold
```

Ingestion loads four source families in sequence: retailer MySQL, supplier
MySQL, ops/storefront files (`sources/product_catalog.xlsx` +
`sources/orders_feed.csv`, standardized to Parquet), and the customer reviews
API. Gold reconciles file data against the database
(`product_reconciliation`, `orders_reconciliation`,
`product_catalog_unified`).

## Run

```sh
cp .env.example .env  # adjust UID/GID if needed
docker compose up --build -d
```

Airflow UI: <http://localhost:8080>

Trigger a run from the CLI:

```sh
docker compose exec airflow airflow dags trigger retail_pipeline
docker compose exec airflow airflow dags list-runs -d retail_pipeline
```

The scheduled parent DAG is `retail_pipeline`. It triggers and waits for:

- `retail_ingestion`: retailer MySQL, supplier MySQL, file sources (Excel/CSV), then customer reviews.
- `retail_transform`: bronze, silver, then gold.

Child DAGs have no schedule. Task retries are bounded and ingestion stays
sequential through the shared DuckDB metadata file.

## Services and output

- Airflow UI: `http://localhost:8080`
- MySQL: `localhost:3306`
- Fake GCS API: `http://localhost:4444`
- Landing objects: `data/retail-datalake/`
- Analytics DuckDB: `output/analytics.duckdb`
- Pipeline DuckDB: `output/pipeline.duckdb` (`audit_log` per-load deltas,
  `pipeline_logs` per-run events, both keyed by `run_id`; `watermark_state`
  for incremental state).

`data/`, `output/`, and Airflow metadata are local runtime state.

## Transform layers

- **Bronze**: raw views over landing Parquet, one per entity, no transformation.
- **Silver**: cleaned data with SCD Type 2 history for customers, orders,
  product-suppliers, and reviews (`*_history` tables + active-only views);
  full-refresh for facts and small dimensions. Invalid rows are flagged with
  `is_quarantined`.
- **Gold**: eight analytics tables — five business marts (sales, customer
  engagement, product performance, supplier analysis, reviews summary), two
  file-vs-database reconciliations, and one unified catalog.

## Tests

```sh
python3 -m pytest tests/ -q
```

`test_scd_contract.py` enforces the structural contract (gold references
active-only views, never `is_active` or `*_history`);
`test_sql_optimizations.py` verifies gold aggregations against fixtures.

## Production mapping

Local services stand in for production boundaries only:

- Fake GCS replaces Google Cloud Storage;
- DuckDB replaces the cloud warehouse for local analytics and metadata;
- Polars replaces Spark for local ingestion;
- Local Airflow replaces the managed orchestrator.

Production deployments should use managed metadata storage, provider
operators for external compute, secret backends, and a production executor.
