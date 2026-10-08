"""Fetch customer reviews from the API to landing as Parquet.

Snapshot ingestion: the API is called once per run and the result is stored
with a dated object name, so transforms never call the API directly.
Run from the repo root: python3 ingestion/customer_reviews_to_landing.py
"""

import datetime
import io
import os

import polars as pl
import requests

from landing_utils import GCS_BUCKET, get_storage_client

API_URL = "https://67e51d5418194932a5849592.mockapi.io/retailer/reviews"
RUN_ID = os.getenv("PIPELINE_RUN_ID") or datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main():
    response = requests.get(API_URL, timeout=30)
    if response.status_code != 200:
        print(f"[x] Failed to fetch data. Status Code: {response.status_code}")
        raise SystemExit(1)
    print(f"[v] Successfully fetched {len(response.json())} records.")

    # pl.read_json takes bytes; a str argument would be treated as a file path.
    df = pl.read_json(response.content)

    today = datetime.datetime.today().strftime("%Y%m%d")
    object_path = f"landing/customer_reviews/customer_reviews_{today}_{RUN_ID}.parquet"

    buffer = io.BytesIO()
    df.write_parquet(buffer)
    get_storage_client().bucket(GCS_BUCKET).blob(object_path).upload_from_string(
        buffer.getvalue(), content_type="application/octet-stream"
    )

    print(f"[v] Wrote {df.height} rows to gs://{GCS_BUCKET}/{object_path}")


if __name__ == "__main__":
    main()
