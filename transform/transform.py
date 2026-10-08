"""Run the medallion layers (bronze -> silver -> gold) on DuckDB.

Executes transform/bronze.sql, transform/silver.sql, and transform/gold.sql
in order. Run from the repo root: python3 transform/transform.py
"""

import argparse
import os
from pathlib import Path

import duckdb

DB_PATH = os.getenv("ANALYTICS_DUCKDB_PATH", "output/analytics.duckdb")
SQL_DIR = Path(os.getenv("TRANSFORM_SQL_DIR", "transform"))
STEPS = ("bronze", "silver", "gold")


def run_step(step: str):
    """Execute one idempotent medallion layer."""
    if step not in STEPS:
        raise ValueError(f"unknown transform layer: {step}")

    SQL_DIR.joinpath(f"{step}.sql").read_text(encoding="utf-8")
    with duckdb.connect(DB_PATH) as con:
        print(f"==> {step.upper()}: running {step}.sql")
        con.execute(SQL_DIR.joinpath(f"{step}.sql").read_text(encoding="utf-8"))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=STEPS)
    args = parser.parse_args()

    steps = (args.step,) if args.step else STEPS
    for step in steps:
        run_step(step)

    if args.step:
        return

    print(f"\n[v] Transform done! Database: {DB_PATH}")


if __name__ == "__main__":
    main()
