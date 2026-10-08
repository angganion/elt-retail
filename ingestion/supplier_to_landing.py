"""Extract supplier MySQL database to landing.

Same structure as retailer_to_landing.py; driven by
configs/supplier_config.csv. Run from the repo root:
python3 ingestion/supplier_to_landing.py
"""

import os

from sqlalchemy import create_engine

from landing_utils import (
    extract_and_save_to_landing,
    init_metadata_tables,
    read_config_file,
    save_logs_to_duckdb,
)

MYSQL_URI = os.getenv(
    "SUPPLIER_MYSQL_URI",
    "mysql+pymysql://project:project@127.0.0.1:3306/supplierdb",
)
CONFIG_FILE = "configs/supplier_config.csv"
SRC = "supplier-db"


def main():
    init_metadata_tables()
    engine = create_engine(MYSQL_URI)
    log_entries = []

    config_df = read_config_file(CONFIG_FILE)
    for row in config_df.iter_rows(named=True):
        if str(row["is_active"]) != "1":
            print(f"[>] Skipping {row['table']} (is_active={row['is_active']})")
            continue

        extract_and_save_to_landing(
            engine, SRC, row["table"], row["load_type"], row["watermark"], log_entries
        )

    save_logs_to_duckdb(log_entries)
    print("[v] Supplier pipeline completed!")


if __name__ == "__main__":
    main()
