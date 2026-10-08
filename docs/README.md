# ELT Retail — Technical Reference

Local ELT pipeline: MySQL and file/API sources → GCS landing (Parquet) →
DuckDB medallion layers (bronze → silver → gold), orchestrated by Airflow.

The seed dataset is small and synthetic, intended for pipeline development
and verification rather than performance benchmarking.

---

## DATASET

- 2 tables from MySQL: **retailer** and **supplier**
- 1 external API: **customer reviews**
- 2 files from outside systems: **product catalog (Excel)** and **orders feed (CSV)**

---

## FLOW

### 1. INGEST

The ingestion layer collects data from four sources: the retailer MySQL database, the supplier MySQL database, the customer reviews API, and two business files (an Excel catalog export and a CSV orders feed). All extracted data is written to GCS first. DuckDB does not connect directly to MySQL or the API during transformation.

**1.1 Customer reviews API**

The API ingestion uses `requests` to fetch the reviews endpoint. The response is converted to a Pandas DataFrame and written as a dated Parquet file in `/tmp`. That file is then uploaded to the customer reviews landing path in GCS:

```text
landing/customer_reviews/customer_reviews_YYYYMMDD.parquet
```

This is a snapshot ingestion. The API response is fetched at a point in time and stored with the date in its filename, so the transformation layer can use the saved snapshot without calling the API again. Parquet is used here because the response is tabular and the file can be read efficiently by analytical tools.

**1.2 Retailer and supplier MySQL**

The retailer and supplier databases use the same Spark JDBC extraction pattern, but each has its own database connection and configuration file. The configuration file stored in GCS defines which tables are active, whether each table uses a full or incremental load, which column is used as its watermark, and where the data belongs. This keeps the extraction logic generic instead of writing a separate query for every table.

For every active table, the job:

1. reads the latest audit information from BigQuery;
2. reads the complete table for a full load, or only rows newer than the latest watermark for an incremental load;
3. converts the result to Parquet;
4. uploads the Parquet file to the table's landing path in GCS.

The MySQL files are stored using this layout:

```text
landing/<database>-db/<table>/<table>_DDMMYYYY.parquet
```

The retailer database contains products, categories, customers, orders, and order items. The supplier database contains suppliers and the relationship between suppliers and products. Tables with a suitable timestamp column can use incremental loading, while a full load is used when a complete snapshot is more appropriate.

Before writing a new table file, existing Parquet files for that table are copied to an archive path and removed from the landing path. The archive is organized by table and date so older raw extracts remain available for reprocessing:

```text
landing/<database>-db/archive/<table>/YYYY/MM/DD/<file>.parquet
```

**1.3 File sources (Excel + CSV)**

Not every source is a database or an API. In real companies, teams send files: an operations team exports an Excel catalog, and the storefront dumps a CSV of orders. `sources/` holds two such files, and `ingestion/file_sources_to_landing.py` standardizes them into the same landing contract (Parquet):

- `product_catalog.xlsx` — the header is not in row 1 (there are title rows above it), prices are text (`$499.00`), dates are Excel serial numbers (e.g. `46265`), and names carry extra whitespace. It partly overlaps the database, adds three products and a `stock_on_hand` column that MySQL does not have, and misses one product.
- `orders_feed.csv` — renamed columns (`order_no`, `amount`), `DD/MM/YYYY` dates, and a thousands separator (`"1,020.00"`). It partly overlaps the database and adds three online orders that have not reached the ERP yet.

Fixing these formats is part of the ingestion job: the rest of the pipeline only sees clean, typed Parquet. Because the files both overlap and extend the database, gold also has reconciliation tables (`product_reconciliation`, `orders_reconciliation`, `product_catalog_unified`) that report matched, mismatched, database-only, and file-only rows.

```text
landing/file-sources/<name>/<name>_DDMMYYYY.parquet
```

**1.4 Audit and logging**

The ingestion writes two kinds of operational information. For each table load, an audit row is appended to BigQuery with the table name, load type, record count, load timestamp, and status. This gives the next run a reference for incremental extraction and provides a history of what was loaded.

The job also keeps a list of log events in memory. At the end of the run, those events are saved both as a JSON file in GCS and as rows in a BigQuery logging table. GCS keeps the file-level record, while BigQuery makes the execution history easier to query.

**Why use GCS as the landing layer?**

GCS separates extraction from transformation. MySQL and the API only need to be accessed during ingestion; DuckDB can work from the files that have already been landed. This also gives the project raw historical files that can be reprocessed or backfilled without querying the source systems again.

All landing files use Parquet, a columnar format: each column is stored together, which makes analytical queries faster (DuckDB only reads the columns it needs), preserves the original data types (a DECIMAL stays a DECIMAL instead of becoming a string), and compresses well. This is the standard format for data lakes. (The original template used JSON Lines for the MySQL extracts because it is simple and readable, but the local version standardizes on Parquet.)

---

### 2. TRANSFORM

Once the raw files are in GCS, DuckDB reads the landing Parquet files with `read_parquet()`. SQL transformations are then applied to clean, join, and reshape the data, for example joining retailer products with supplier information and enriching the result with customer reviews.

---

### 3. LOAD

The final transformed result is written back to DuckDB as a persistent table, ready for analysis or reporting.

---

## CORE CONCEPTS (for beginners)

Short explanations of the ideas this project teaches. Each concept maps to real code in this repo.

### ELT (not ETL)

This project is **E**xtract-**L**oad-**T**ransform: we first copy raw data from
the sources into a landing area (Load), and only afterwards
clean/join/aggregate it (Transform). The alternative, ETL, transforms *before*
loading — which can make transformation a bottleneck and harder to scale when
data grows, because the transform step runs on a separate server before data
reaches the warehouse. Modern data stacks often prefer ELT because the raw
data stays available in the warehouse and transformation can be re-run
anytime. That said, ETL still has legitimate uses (regulatory PII scrubbing,
legacy on-prem warehouses, IoT edge filtering) — the choice is a trade-off,
not a universal rule. See `ingestion/` (the EL) and `transform/` (the T).

### Landing layer

A storage area that holds **raw, unchanged** data. Sources (MySQL, API) are only touched during ingestion; everything downstream reads from the landing files. This decouples extraction from transformation and gives you historical raw files that can be reprocessed or backfilled without querying the sources again.

### Full load vs incremental load (watermark)

- **Full load**: copy the whole table every run. Simple, but wasteful for big tables.
- **Incremental load**: copy only rows *newer than the last load*. The marker used to know "what was already loaded" is called a **watermark** — here it is the latest `load_timestamp` in the audit table (`output/pipeline.duckdb`).

The config files (`configs/*.csv`) decide per table which strategy to use and which column is the watermark. You can see it live: run the pipeline twice and watch the incremental tables report `No new data ... landing file kept as-is`.

### SCD Type 2 (Slowly Changing Dimension)

When a dimension changes (e.g. a customer's email), a **Type 2** dimension does not overwrite the old row — it closes the old version (`is_active = false`, `effective_end_date` set) and inserts a new active version. You keep a complete history of changes. The `silver` layer implements this with two `MERGE` statements per table (see `transform/silver.sql` for why two are needed).

### Medallion architecture (bronze / silver / gold)

A popular data-lake pattern with three layers:

- **Bronze** — raw data as landed, nothing changed. In this repo: views pointing at the landing files.
- **Silver** — cleaned, conformed, with history (SCD) and quality flags (`is_quarantined`).
- **Gold** — aggregated, business-ready tables for dashboards/analytics.

### Snapshot ingestion

The reviews API is called once per run and stored with the date in its filename (`customer_reviews_YYYYMMDD.parquet`). This is a **snapshot**: downstream layers use the saved file, not the live API, so results are reproducible and the API isn't hammered.

### Archive & reprocessing

Before writing a new snapshot to landing, the previous one is **archived** (copied to `landing/<src>-db/archive/<table>/YYYY/MM/DD/` and removed from landing). Raw data is never thrown away — this is what enables reprocessing/backfill later.

### Config-driven pipeline

The config CSVs decide which tables run (`is_active`), the load strategy, and the watermark column — without touching code. Turning a table off is one edit in a CSV. This is a common pattern to make pipelines maintainable.

### Columnar format (Parquet) vs row format (JSON Lines)

Parquet stores data **column by column**: analytical queries read only the needed columns, data types are preserved (a DECIMAL stays a DECIMAL), and compression is better. That's why it's the standard for data lakes. JSON Lines is human-readable and flexible, but loses types and is slower/space-heavier for analytics. This repo standardizes on Parquet everywhere.

### Emulator pattern

`fake-gcs-server` emulates Google Cloud Storage, and the code talks to it through the **official** `google-cloud-storage` library by setting `STORAGE_EMULATOR_HOST`. Remove that env var (and use real credentials) and the exact same code runs against real GCS — a common technique for developing and testing cloud code locally.

---

## LOCAL SETUP (THIS REPO)

The ingestion scripts live in `ingestion/`, adapted from the original Spark + GCS + BigQuery version to local technologies:

| Original (cloud) | Local equivalent |
|---|---|
| GCS landing | `fake-gcs-server` emulator (Docker). Uploads via `google-cloud-storage` pointed at `STORAGE_EMULATOR_HOST`; objects persist as files under `data/` |
| BigQuery audit & logs | DuckDB `output/pipeline.duckdb` |
| Spark JDBC | Polars `pl.read_database()` + SQLAlchemy |
| Config file in GCS | `configs/*.csv` |

### Run

```sh
# Pastikan container fake-gcs berjalan dengan UID yang sama dengan user host,
# supaya file hasil upload bisa dibaca dari host:
export UID=$(id -u); export GID=$(id -g)

docker compose up -d mysql fake-gcs   # start MySQL + fake GCS emulator
pip install -r requirements.txt

python3 ingestion/retailer_to_landing.py
python3 ingestion/supplier_to_landing.py
python3 ingestion/file_sources_to_landing.py
python3 ingestion/customer_reviews_to_landing.py

# Atau langsung semuanya sekaligus (start containers + 4 script berurutan):
./run_all.sh
```

Run everything from the repo root so the relative paths (`configs/`, `output/`) work. The MySQL scripts read their config CSV to decide which tables are active and whether each uses a full or incremental load (see the `is_active` and `load_type` columns).

Uploaded objects land in the `retail-datalake` bucket (`data/retail-datalake/` on disk) with a uniform layout: `landing/<src>-db/<table>/<table>_DDMMYYYY.parquet`, `landing/customer_reviews/customer_reviews_YYYYMMDD.parquet`, `landing/file-sources/<name>/<name>_DDMMYYYY.parquet`, and archives under `landing/<src>-db/archive/<table>/YYYY/MM/DD/`.

### Transform (medallion)

The `transform/` folder holds the medallion SQL, executed in DuckDB:

| Layer | Content |
|---|---|
| Bronze | Raw views over landing files (`data/.../*.parquet`) — data is never copied |
| Silver | Clean tables + SCD Type 2 (`*_history` + active-only views) + `is_quarantined` flag |
| Gold | 8 tables: 5 business aggregates plus 3 file-vs-database tables (`product_reconciliation`, `orders_reconciliation`, `product_catalog_unified`), all `CREATE OR REPLACE TABLE` so reruns refresh |

```sh
python3 transform/transform.py
```

For the implemented query performance changes and their verification steps, see **[SQL Optimization Guide](SQL_OPTIMIZATION.md)**.

Results are written to `output/analytics.duckdb` in the `bronze`, `silver`, and `gold` schemas. Query them, e.g.:

```sh
python3 -c "import duckdb; print(duckdb.connect('output/analytics.duckdb').execute('SELECT * FROM gold.sales_summary ORDER BY total_sales DESC').fetchall())"
```

Note: `bronze.*` are views that read landing files from host paths (`data/retail-datalake/...`), so they only work when queried from the host (e.g., `python3 transform/transform.py` or a local duckdb CLI). The `silver` and `gold` tables are materialized inside the DuckDB file, so they can be queried from the DuckDB container as shown above.

---

## GCS SETUP (fake-gcs emulator)

The `fake-gcs-server` Docker container mimics Google Cloud Storage locally. The ingestion scripts talk to it with the **same** `google-cloud-storage` library used in the original cloud version — only the endpoint changes.

### How it works

- The emulator runs with a **filesystem backend** rooted at `/data` (`docker-compose.yml`: `-backend filesystem -filesystem-root /data`).
- `./data` is bind-mounted read-write, so uploaded objects are **real files on disk** under `data/<bucket>/...` and survive container restarts.
- The container runs as your host user (`user: ${UID:-1000}:${GID:-1000}`) so uploaded files stay readable from the host. If your user is not UID 1000, export it before `docker compose up`: `export UID=$(id -u); export GID=$(id -g)`.

### Connecting from Python

The ingestion scripts connect like this (see `ingestion/landing_utils.py`):

```python
import os
os.environ["STORAGE_EMULATOR_HOST"] = "http://localhost:4443"  # arahkan ke emulator

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

client = storage.Client(project="test", credentials=AnonymousCredentials())
```

`STORAGE_EMULATOR_HOST` redirects the official Google client to the emulator. The same code runs against real GCS by removing that line, swapping `AnonymousCredentials` for a service account, and using the real project.

### Creating the bucket

The bucket **`retail-datalake` is created automatically** the first time any ingestion script runs (`create_bucket()` in `ingestion/landing_utils.py` — the emulator does **not** auto-create buckets, same as real GCS).

To create it manually instead, either:

**Option A — make the folder** (the filesystem backend treats directories as buckets):

```sh
mkdir -p data/retail-datalake
```

**Option B — via the JSON API** (the same call the Python client makes):

```sh
curl -X POST http://localhost:4443/storage/v1/b \
  -H 'Content-Type: application/json' \
  -d '{"name":"retail-datalake"}'
```

### Inspecting the emulator

```sh
# list buckets
curl http://localhost:4443/storage/v1/b

# list objects in a bucket
curl 'http://localhost:4443/storage/v1/b/retail-datalake/o'

# download one object (JSON API URL pattern)
curl 'http://localhost:4443/download/storage/v1/b/retail-datalake/o/landing/retailer-db/products/products_08082026.parquet?alt=media'
```

Or just look at the files on disk under `data/retail-datalake/`.

### Object layout

```text
retail-datalake/
├── landing/retailer-db/<table>/<table>_DDMMYYYY.parquet        # 5 tabel retailer
├── landing/supplier-db/<table>/<table>_DDMMYYYY.parquet        # 2 tabel supplier
├── landing/customer_reviews/customer_reviews_YYYYMMDD.parquet  # snapshot API
├── landing/file-sources/<name>/<name>_DDMMYYYY.parquet         # Excel + CSV
├── landing/<src>-db/archive/<table>/YYYY/MM/DD/                # file lama (reprocessing)
└── temp/pipeline_logs/pipeline_log_*.json                      # log pipeline
```

### Reading from DuckDB

Through the emulator HTTP endpoint (same pattern as `query.sql`; use `fake-gcs` instead of `localhost` inside the Docker network):

```sql
SELECT * FROM read_parquet('http://localhost:4443/download/storage/v1/b/retail-datalake/o/landing/retailer-db/products/products_08082026.parquet?alt=media');
```

The `bronze` views in `transform/bronze.sql` instead read the same files directly from disk with a glob (`data/retail-datalake/landing/retailer-db/products/*.parquet`) — DuckDB can glob local paths, but not generic HTTP URLs.

---

## FIRST-TIME SETUP CHECKLIST

1. **Prerequisites**: Docker with Compose, Python 3.10+, `pip`. Check: `docker compose version` and `python3 --version`.
2. **Start the containers** (MySQL + fake GCS emulator):
   ```sh
   export UID=$(id -u); export GID=$(id -g)
   docker compose up -d mysql fake-gcs
   docker compose ps        # both should show "Up"
   ```
3. **Install Python dependencies**:
   ```sh
   pip install -r requirements.txt
   ```
4. **Run the whole pipeline** via Airflow (trigger `retail_pipeline`), or step by step:
   ```sh
   python3 ingestion/retailer_to_landing.py
   python3 ingestion/supplier_to_landing.py
   python3 ingestion/file_sources_to_landing.py
   python3 ingestion/customer_reviews_to_landing.py
   python3 transform/transform.py
   ```
5. **Verify you see**:
   - 8 Parquet objects under `data/retail-datalake/landing/` (7 tables + reviews)
   - `==> 5. Transform ... ✅ Transform selesai!`
   - Gold tables: `python3 -c "import duckdb; con=duckdb.connect('output/analytics.duckdb'); print(con.sql('SELECT * FROM gold.sales_summary'))"`

If MySQL seems empty on the first run, give it a few seconds — the init scripts run once on first container start.

---

## TROUBLESHOOTING

| Problem | Cause | Fix |
|---|---|---|
| `Permission denied` reading files in `data/` or `output/` | Files created by a Docker container running as root (UID mismatch) | Run `export UID=$(id -u); export GID=$(id -g)` **before** `docker compose up`, or fix an existing file: `docker run --rm -v "$PWD/output:/output" mysql:8.0 chown 1000:1000 /output/analytics.duckdb` |
| `Connection refused` on port 3306/4443 | Containers not running | `docker compose up -d mysql fake-gcs` and re-run |
| Bucket error / `404` on upload | Bucket `retail-datalake` doesn't exist yet (emulator does **not** auto-create buckets) | Run any ingestion script once (auto-creates it), or `mkdir -p data/retail-datalake` |
| `❌ Failed to fetch` from the reviews API | No internet, or mockapi is temporarily down | Retry; the endpoint is `https://67e51d5418194932a5849592.mockapi.io/retailer/reviews` |
| `No new data ... landing file kept as-is` | **Not an error** — it means the incremental load found nothing newer than the watermark | This is the expected behavior on re-runs |
| DuckDB `Could not set lock` / file busy | Two scripts writing the same DuckDB file at once | Run scripts sequentially |
| `python3` can't find `polars`/`duckdb` | Dependencies not installed for the interpreter you're using | `pip install -r requirements.txt` (check `which python3`) |
| `globs (*) for generic HTTP file is are not supported` | DuckDB can't glob over plain HTTP URLs | Use an exact object URL, or read from disk (`data/...`) as the bronze views do |
| Gold `avg_rating` is NULL / 0 reviews | The mock API's `product_id`s don't overlap the local products (101–120) — a data quirk, not a bug | Optionally filter by products that actually have reviews |
| Container started but MySQL empty | MySQL init scripts only run on the **first** start of an empty volume | Reset: `docker compose down -v && docker compose up -d mysql` (wipes all data) |
