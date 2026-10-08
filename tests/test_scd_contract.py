"""Structural SCD contract (static test, no DB or server).

Silver base views (customers, orders, ...) expose active rows only with no
is_active column; gold.sql must not reference is_active. Raw history lives in
*_history tables, which gold must not reference either. order_items is a
full-refresh fact, never MERGEd. Text matching is sufficient here because the
rule is structural ("never reference X"), complementing the behavioral tests
in test_sql_optimizations.py.
"""

from pathlib import Path

REPO = Path(__file__).parents[1]
GOLD_SQL = REPO.joinpath("transform", "gold.sql").read_text(encoding="utf-8")
SILVER_SQL = REPO.joinpath("transform", "silver.sql").read_text(encoding="utf-8")


def _code_lines(sql: str) -> str:
    """Return non-comment lines so assertions apply to SQL, not comments."""
    return "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )


def test_gold_has_no_version_filter():
    code = _code_lines(GOLD_SQL)
    assert "is_active" not in code, (
        "gold.sql references is_active: base views have no such column. "
        "Remove the filter (views already guarantee active-only) or, when history "
        "is genuinely required, reference *_history and update this test explicitly."
    )


def test_gold_never_references_raw_history():
    code = _code_lines(GOLD_SQL)
    for forbidden in (
        "customers_history",
        "orders_history",
        "product_suppliers_history",
        "customer_reviews_history",
    ):
        assert forbidden not in code, (
            f"gold.sql references {forbidden}: history tables are for as-of/audit "
            "analysis only, not aggregates. Use the base view instead."
        )


def test_order_items_is_not_scd2():
    code = _code_lines(SILVER_SQL)
    assert "MERGE INTO silver.order_items" not in code, (
        "silver.order_items is MERGEd: it is a full-refresh fact (TRUNCATE+INSERT), "
        "not a dimension. See the order_items block in silver.sql."
    )


def test_history_tables_and_active_views_exist():
    code = _code_lines(SILVER_SQL)
    for entity in ("customers", "orders", "product_suppliers", "customer_reviews"):
        assert f"silver.{entity}_history" in code, f"history table {entity}_history missing"
        assert f"VIEW silver.{entity} AS" in code, f"active-only view {entity} missing"
