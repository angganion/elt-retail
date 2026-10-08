"""Shared helpers for the MySQL -> landing (GCS) pipeline.

Local stack: fake-gcs-server (via STORAGE_EMULATOR_HOST), DuckDB for
audit/log metadata (output/pipeline.duckdb), Polars + SQLAlchemy for extraction.
Uses the standard google-cloud-storage client, so pointing at real GCS only
requires emulator removal and service-account credentials.
"""

import datetime
import io
import os

import duckdb
import polars as pl

os.environ.setdefault("STORAGE_EMULATOR_HOST", "http://localhost:4443")

from google.api_core.exceptions import Conflict
from google.auth.credentials import AnonymousCredentials
from google.cloud import storage

GCS_BUCKET = os.getenv("GCS_BUCKET", "retail-datalake")
_storage_client = None
RUN_ID = os.getenv("PIPELINE_RUN_ID") or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def get_storage_client():
    """Return a lazily created GCS client, creating the bucket if needed."""
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project="test", credentials=AnonymousCredentials())
        try:
            _storage_client.create_bucket(GCS_BUCKET)
            print(f"[v] Bucket gs://{GCS_BUCKET} created")
        except Conflict:
            pass
    return _storage_client


DUCKDB_PATH = os.getenv("PIPELINE_DUCKDB_PATH", "output/pipeline.duckdb")
DEFAULT_WATERMARK = datetime.datetime(1900, 1, 1)


def init_metadata_tables():
    """Create audit_log, pipeline_logs, and watermark_state if missing.

    audit_log stores one row per load (delta row count for this run, not a
    cumulative size). pipeline_logs stores per-run events; both join on run_id.
    watermark_state holds incremental-load state. The ALTER statements migrate
    databases created before run_id existed.
    """
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
            run_id VARCHAR,
            tablename VARCHAR,
            load_type VARCHAR,
            record_count BIGINT,
            load_timestamp TIMESTAMP,
            status VARCHAR
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_logs (
            run_id VARCHAR,
            timestamp VARCHAR,
            event_type VARCHAR,
            message VARCHAR,
            "table" VARCHAR
        )
        """
    )
    con.execute("ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS run_id VARCHAR")
    con.execute("ALTER TABLE pipeline_logs ADD COLUMN IF NOT EXISTS run_id VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS watermark_state (
            tablename VARCHAR PRIMARY KEY,
            watermark_value TIMESTAMP,
            updated_at TIMESTAMP
        )
        """
    )
    con.close()


def log_event(log_entries, event_type, message, table=None):
    """Append one event to the in-memory batch and print it.

    Entries are flushed once per run by save_logs_to_duckdb.
    """
    entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table,
    }
    log_entries.append(entry)
    print(f"[{entry['timestamp']}] {event_type} - {message}")


def save_logs_to_duckdb(log_entries):
    """Flush buffered events to pipeline_logs. Example:

        SELECT * FROM pipeline_logs ORDER BY timestamp DESC;
    """
    if not log_entries:
        return
    con = duckdb.connect(DUCKDB_PATH)
    con.executemany(
        'INSERT INTO pipeline_logs (run_id, timestamp, event_type, message, "table") VALUES (?, ?, ?, ?, ?)',
        [(RUN_ID, e["timestamp"], e["event_type"], e["message"], e["table"]) for e in log_entries],
    )
    con.close()
    print(f"[v] {len(log_entries)} logs inserted into {DUCKDB_PATH} (pipeline_logs)")


def read_config_file(path):
    """Read the pipeline table config CSV (tables, load types, watermark columns).

    New tables are onboarded by adding CSV rows; no code changes required.
    """
    return pl.read_csv(path, has_header=True)


def move_existing_files_to_archive(src, table, log_entries):
    """Move current landing objects to a date-partitioned archive folder.

    Landing keeps the latest snapshot; previous objects are archived for
    reprocessing instead of deleted. Raw data is never discarded.
    """
    client = get_storage_client()
    bucket = client.bucket(GCS_BUCKET)

    blobs = list(client.list_blobs(GCS_BUCKET, prefix=f"landing/{src}/{table}/"))
    existing = [b.name for b in blobs if b.name.endswith(".parquet")]
    if not existing:
        return

    for name in existing:
        archive_path = f"landing/{src}/archive/{table}/{RUN_ID}/{name.split('/')[-1]}"

        # Object storage has no rename; copy + delete implements the move.
        bucket.copy_blob(bucket.blob(name), bucket, archive_path)
        bucket.blob(name).delete()
        log_event(log_entries, "INFO", f"Moved {name} to {archive_path}", table=table)


def save_to_landing(src, table, df, load_type, log_entries, watermark=None):
    """Write a DataFrame to landing as Parquet and record the audit row.

    Shared by database and file ingestion: archive the previous snapshot,
    upload the new Parquet object, then append the audit entry.
    """
    move_existing_files_to_archive(src, table, log_entries)

    # Parquet preserves types, scans columnar for analytics, and compresses
    # well; it is the standard data lake format used for all landing data.
    today = datetime.datetime.now().strftime("%d%m%Y")
    object_path = f"landing/{src}/{table}/{table}_{today}_{RUN_ID}.parquet"
    buffer = io.BytesIO()
    df.write_parquet(buffer)

    client = get_storage_client()
    client.bucket(GCS_BUCKET).blob(object_path).upload_from_string(
        buffer.getvalue(), content_type="application/octet-stream"
    )
    log_event(log_entries, "SUCCESS", f"Wrote {df.height} rows to gs://{GCS_BUCKET}/{object_path}", table=table)

    append_audit(table, load_type, df.height, "SUCCESS", watermark)
    log_event(log_entries, "SUCCESS", f"Audit row appended for {table}", table=table)


def get_latest_watermark(table_name):
    """Return the last stored watermark, or the 1900 default (take all) on first run."""
    con = duckdb.connect(DUCKDB_PATH)
    row = con.execute(
        "SELECT watermark_value FROM watermark_state WHERE tablename = ?",
        [table_name],
    ).fetchone()
    con.close()
    if row is None or row[0] is None:
        return DEFAULT_WATERMARK
    return row[0]


def append_audit(table, load_type, record_count, status, watermark=None):
    """Insert one audit_log row (delta count for this run) and upsert the watermark.

    Columns are named explicitly so added columns never shift positions.
    Called after a successful upload so state stays consistent with landing.
    """
    con = duckdb.connect(DUCKDB_PATH)
    con.execute(
        "INSERT INTO audit_log (run_id, tablename, load_type, record_count, load_timestamp, status)"
        " VALUES (?, ?, ?, ?, ?, ?)",
        [RUN_ID, table, load_type, record_count, datetime.datetime.now(), status],
    )
    if watermark is not None:
        con.execute(
            """
            INSERT INTO watermark_state VALUES (?, ?, ?)
            ON CONFLICT (tablename) DO UPDATE SET watermark_value = excluded.watermark_value,
                updated_at = excluded.updated_at
            """,
            [table, watermark, datetime.datetime.now()],
        )
    con.close()


def extract_and_save_to_landing(engine, src, table, load_type, watermark_col, log_entries):
    """Extract from MySQL and write to landing (the E in ELT).

    Full load selects all rows; incremental selects rows newer than the last
    watermark. Empty incremental results keep the existing landing file as-is.
    """
    try:
        last_watermark = None
        if load_type.lower() == "incremental":
            last_watermark = get_latest_watermark(table)
            log_event(log_entries, "INFO", f"Latest watermark for {table}: {last_watermark}", table=table)

        if load_type.lower() == "full load":
            query = f"SELECT * FROM {table}"
        else:
            # Watermark values originate from the config CSV, not user input.
            wm = (
                last_watermark.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(last_watermark, datetime.datetime)
                else str(last_watermark)
            )
            query = f"SELECT * FROM {table} WHERE {watermark_col} > '{wm}'"

        df = pl.read_database(query, engine)
        log_event(log_entries, "SUCCESS", f"Extracted {df.height} rows from {table}", table=table)

        if df.height == 0:
            log_event(
                log_entries,
                "INFO",
                f"No new data for {table} since last watermark — landing file kept as-is",
                table=table,
            )
            return

        source_watermark = df[watermark_col].max() if watermark_col and watermark_col in df.columns else None
        save_to_landing(src, table, df, load_type, log_entries, source_watermark)

    except Exception as e:
        log_event(log_entries, "ERROR", f"Error processing {table}: {e}", table=table)
        raise
