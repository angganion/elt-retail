# SQL Optimization Guide

This project uses DuckDB for the medallion transformation and MySQL as the operational source. The optimizations below reduce intermediate rows and repeated file scans without changing the bronze/silver architecture.

## Implemented changes

### Customer engagement

`transform/gold.sql` now aggregates active order items into one row per order before joining to customers. This means:

- `total_orders` counts orders rather than order-item rows.
- `total_spent` remains the sum of active order-item values.
- `avg_order_value` is the average value of complete orders.
- Orders without active items contribute an order value of zero.

Previously, joining `orders` directly to `order_items` caused one order with two items to be counted twice.

### Product performance

Sales and reviews are now aggregated independently by product before joining to product and supplier dimensions. This prevents the `order_items × customer_reviews` fanout from inflating sales and review counts when a product has both multiple items and multiple reviews.

The output still contains one row per active product-supplier relationship. Product sales remain attributed to every supplier associated with that product; allocating sales between suppliers is not part of this change.

### Supplier analysis

Supplier analysis reuses a product-level sales aggregate before joining to supplier relationships. The existing supplier attribution behavior is preserved, but raw order-item rows are no longer carried through the supplier join.

### Silver SCD source staging

The five SCD inputs that are consumed by two `MERGE` statements are materialized into temporary deduplicated tables once per transform connection:

- customers
- orders
- order items
- product suppliers
- customer reviews

Both the closing and insertion `MERGE` statements reuse those snapshots. Temporary tables disappear when `transform.py` closes its DuckDB connection; they are not part of the persisted analytics database.

### MySQL watermark indexes

The MySQL initialization scripts now define B-tree indexes for the configured incremental predicates:

- `products(updated_at)`
- `orders(updated_at)`
- `order_items(updated_at)`
- `product_suppliers(last_updated)`

These indexes support the existing `WHERE watermark_column > last_watermark` extraction queries. Existing Docker volumes are not reinitialized automatically, so a running environment needs an explicit DDL migration or a volume recreation before the init-script indexes appear.

## Verification

Run the aggregate regression tests:

```sh
python3 -m unittest tests.test_sql_optimizations -v
```

Run the complete local transformation:

```sh
python3 transform/transform.py
```

The smoke test verifies bronze, silver, and all five gold tables. The regression tests cover:

- one order with multiple items is counted once;
- order-level spending and average order value are correct;
- sales and reviews do not multiply through an independent join;
- supplier-level attribution remains intact.

For MySQL source validation, run `EXPLAIN` against the incremental extraction queries and confirm the watermark indexes appear in `possible_keys`; on tiny tables, MySQL may still choose a sequential scan because it is cheaper. The checked-in seed data is intentionally small, so current local execution time is not a meaningful production benchmark; validate with representative table volumes before tuning further.

## Deliberate non-changes

- Bronze remains a view over current landing Parquet files.
- The two-`MERGE` SCD Type 2 pattern remains in place.
- Full-refresh dimensions remain full refreshes because the current project data is small.
- No DuckDB indexes were added; columnar scans and hash joins are appropriate for the current analytical workload.
