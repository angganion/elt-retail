from pathlib import Path
import unittest

import duckdb


class GoldAggregationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Fixtures intentionally omit is_active: gold must run without any
        # version filter since the base views have no such column.
        cls.con = duckdb.connect()
        cls.con.execute("CREATE SCHEMA silver")
        cls.con.execute("CREATE TABLE silver.customers (customer_id BIGINT, name VARCHAR)")
        cls.con.execute(
            "CREATE TABLE silver.orders (order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL)"
        )
        cls.con.execute(
            "CREATE TABLE silver.order_items "
            "(order_item_id BIGINT, order_id BIGINT, product_id BIGINT, quantity BIGINT, price DECIMAL, is_quarantined BOOLEAN)"
        )
        cls.con.execute(
            "CREATE TABLE silver.products "
            "(product_id BIGINT, name VARCHAR, category_id BIGINT, price DECIMAL, is_quarantined BOOLEAN)"
        )
        cls.con.execute("CREATE TABLE silver.categories (category_id BIGINT, name VARCHAR)")
        cls.con.execute(
            "CREATE TABLE silver.product_suppliers "
            "(supplier_id BIGINT, product_id BIGINT)"
        )
        cls.con.execute("CREATE TABLE silver.suppliers (supplier_id BIGINT, supplier_name VARCHAR, is_quarantined BOOLEAN)")
        cls.con.execute(
            "CREATE TABLE silver.customer_reviews "
            "(id VARCHAR, product_id BIGINT, rating BIGINT, review_text VARCHAR)"
        )
        cls.con.execute(
            "CREATE TABLE silver.product_catalog "
            "(product_id BIGINT, name VARCHAR, category_name VARCHAR, price DECIMAL, "
            "stock_on_hand BIGINT, updated_at DATE, is_quarantined BOOLEAN)"
        )
        cls.con.execute(
            "CREATE TABLE silver.orders_feed "
            "(order_id BIGINT, customer_id BIGINT, order_date DATE, total_amount DECIMAL, is_quarantined BOOLEAN)"
        )

        cls.con.execute("INSERT INTO silver.customers VALUES (1, 'Customer 1')")
        cls.con.execute("INSERT INTO silver.orders VALUES (1001, 1, DATE '2026-08-01', 30)")
        cls.con.execute(
            "INSERT INTO silver.order_items VALUES "
            "(1, 1001, 101, 1, 10, FALSE), (2, 1001, 101, 2, 10, FALSE)"
        )
        cls.con.execute("INSERT INTO silver.products VALUES (101, 'Product 101', 1, 10, FALSE)")
        cls.con.execute("INSERT INTO silver.categories VALUES (1, 'Category 1')")
        cls.con.execute("INSERT INTO silver.product_suppliers VALUES (1, 101)")
        cls.con.execute("INSERT INTO silver.suppliers VALUES (1, 'Supplier 1', FALSE)")
        cls.con.execute(
            "INSERT INTO silver.customer_reviews VALUES "
            "('r1', 101, 4, 'Good'), ('r2', 101, 5, 'Great')"
        )
        cls.con.execute(
            "INSERT INTO silver.product_catalog VALUES "
            "(101, 'Product 101', 'Category 1', 10, 5, DATE '2026-08-31', FALSE), "
            "(102, 'Product 102', 'Category 1', 20, 7, DATE '2026-08-31', FALSE)"
        )
        cls.con.execute(
            "INSERT INTO silver.orders_feed VALUES "
            "(1001, 1, DATE '2026-08-01', 30, FALSE), (1002, 1, DATE '2026-08-02', 15, FALSE)"
        )

        gold_sql = Path(__file__).parents[1].joinpath("transform", "gold.sql").read_text(encoding="utf-8")
        cls.con.execute(gold_sql)

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def test_customer_engagement_aggregates_orders_before_items(self):
        row = self.con.execute(
            """
            SELECT total_orders, total_spent, avg_order_value
            FROM gold.customer_engagement
            WHERE customer_id = 1
            """
        ).fetchone()

        self.assertEqual(row[0], 1)
        self.assertAlmostEqual(float(row[1]), 30.0)
        self.assertAlmostEqual(float(row[2]), 30.0)

    def test_product_performance_aggregates_sales_and_reviews_independently(self):
        row = self.con.execute(
            """
            SELECT total_units_sold, total_revenue, avg_rating, total_reviews
            FROM gold.product_performance
            WHERE product_id = 101 AND supplier_id = 1
            """
        ).fetchone()

        self.assertEqual(row[0], 3)
        self.assertAlmostEqual(float(row[1]), 30.0)
        self.assertAlmostEqual(float(row[2]), 4.5)
        self.assertEqual(row[3], 2)

    def test_supplier_analysis_preserves_per_supplier_attribution(self):
        row = self.con.execute(
            """
            SELECT total_products_supplied, total_units_sold, total_revenue
            FROM gold.supplier_analysis
            WHERE supplier_id = 1
            """
        ).fetchone()

        self.assertEqual(row[0], 1)
        self.assertEqual(row[1], 3)
        self.assertAlmostEqual(float(row[2]), 30.0)

    def test_product_reconciliation_flags_matched_and_file_only(self):
        rows = dict(
            self.con.execute(
                "SELECT product_id, match_status FROM gold.product_reconciliation"
            ).fetchall()
        )

        self.assertEqual(rows[101], "matched")
        self.assertEqual(rows[102], "file_only")

    def test_orders_reconciliation_flags_matched_and_file_only(self):
        rows = dict(
            self.con.execute(
                "SELECT order_id, match_status FROM gold.orders_reconciliation"
            ).fetchall()
        )

        self.assertEqual(rows[1001], "matched")
        self.assertEqual(rows[1002], "file_only")

    def test_product_catalog_unified_keeps_file_only_products(self):
        rows = {
            row[0]: (row[1], row[2])
            for row in self.con.execute(
                "SELECT product_id, source_system, stock_on_hand FROM gold.product_catalog_unified"
            ).fetchall()
        }

        self.assertEqual(rows[101][0], "database")
        self.assertEqual(rows[102][0], "file_only")
        self.assertEqual(rows[102][1], 7)


if __name__ == "__main__":
    unittest.main()
