"""Ingest file sources (Excel + CSV) to landing as Parquet.

Sources:
- product_catalog.xlsx: ops team catalog export. Header on row 4, prices as
  "$499.00" strings, dates as Excel serials, padded product names.
- orders_feed.csv: storefront order export. Columns order_no/amount,
  DD/MM/YYYY dates, thousand-separated amounts.

Both partly overlap the DB tables and carry records missing on either side;
gold reconciles the two lineages. Run from the repo root:
python3 ingestion/file_sources_to_landing.py
"""

import datetime
import os
from pathlib import Path

import polars as pl

from landing_utils import (
    init_metadata_tables,
    log_event,
    save_logs_to_duckdb,
    save_to_landing,
)

SOURCES_DIR = Path(os.getenv("FILE_SOURCES_DIR", "sources"))
SRC = "file-sources"
EXCEL_EPOCH = datetime.date(1899, 12, 30)


def read_product_catalog():
    """Read the ops Excel export into the standard landing columns."""
    df = pl.read_excel(
        SOURCES_DIR / "product_catalog.xlsx",
        read_options={"header_row": 3},
    )

    df = df.rename(
        {
            "SKU": "product_id",
            "Product Name": "name",
            "Category": "category_name",
            "Unit Price": "price",
            "Stock On Hand": "stock_on_hand",
            "Last Updated": "updated_at",
        }
    )

    return df.with_columns(
        pl.col("name").str.strip_chars(),
        pl.col("price").cast(pl.Utf8).str.replace_all(r"[$,]", "").cast(pl.Float64),
        (pl.lit(EXCEL_EPOCH) + pl.duration(days=pl.col("updated_at"))).cast(pl.Date).alias("updated_at"),
    )


def read_orders_feed():
    """Read the storefront CSV into the standard landing columns."""
    df = pl.read_csv(SOURCES_DIR / "orders_feed.csv")

    df = df.rename({"order_no": "order_id", "amount": "total_amount"})

    return df.with_columns(
        pl.col("customer_id").cast(pl.Int64),
        pl.col("order_date").str.strptime(pl.Date, "%d/%m/%Y"),
        pl.col("total_amount").cast(pl.Utf8).str.replace_all(",", "").cast(pl.Float64),
    )


def main():
    init_metadata_tables()

    log_entries = []

    catalog = read_product_catalog()
    log_event(log_entries, "SUCCESS", f"Extracted {catalog.height} rows from product_catalog.xlsx", table="product_catalog")
    save_to_landing(SRC, "product_catalog", catalog, "file snapshot", log_entries)

    orders = read_orders_feed()
    log_event(log_entries, "SUCCESS", f"Extracted {orders.height} rows from orders_feed.csv", table="orders_feed")
    save_to_landing(SRC, "orders_feed", orders, "file snapshot", log_entries)

    save_logs_to_duckdb(log_entries)
    print("[v] File-source pipeline completed!")


if __name__ == "__main__":
    main()
