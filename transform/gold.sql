-- Gold: analytics-ready aggregate tables. Idempotent (CREATE OR REPLACE).
-- Silver inputs are active-only views, so no version filter is needed here;
-- the only quality filter is is_quarantined.
-- Facts are pre-aggregated before joins to avoid N x M row multiplication
-- when a product has N order_items and M reviews. Supplier revenue attributes
-- each product total to every active supplier relationship by design.

CREATE SCHEMA IF NOT EXISTS gold;

-- 1. sales_summary: sales per date, category, and product.
-- Grain: order_date x product. total_sales = SUM(price * quantity).
-- unique_customers uses COUNT(DISTINCT) so multi-product buyers count once.
CREATE OR REPLACE TABLE gold.sales_summary AS
SELECT
    o.order_date,
    p.category_id,
    c.name AS category_name,
    oi.product_id,
    p.name AS product_name,
    SUM(oi.quantity) AS total_units_sold,
    SUM(oi.price * oi.quantity) AS total_sales,
    COUNT(DISTINCT o.customer_id) AS unique_customers
FROM silver.orders o
JOIN silver.order_items oi ON o.order_id = oi.order_id
JOIN silver.products p ON oi.product_id = p.product_id
JOIN silver.categories c ON p.category_id = c.category_id
WHERE NOT oi.is_quarantined
GROUP BY 1, 2, 3, 4, 5;

-- 2. customer_engagement: per-customer behavior.
-- Grain: customer. Orders without items count with value 0 (LEFT JOIN + COALESCE).
CREATE OR REPLACE TABLE gold.customer_engagement AS
WITH order_totals AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        COALESCE(SUM(oi.price * oi.quantity), 0) AS order_value
    FROM silver.orders o
    LEFT JOIN silver.order_items oi
        ON o.order_id = oi.order_id AND NOT oi.is_quarantined
    GROUP BY o.order_id, o.customer_id, o.order_date
)
SELECT
    c.customer_id,
    c.name AS customer_name,
    COUNT(ot.order_id) AS total_orders,
    SUM(ot.order_value) AS total_spent,
    MAX(ot.order_date) AS last_order_date,
    date_diff('day', MAX(ot.order_date)::DATE, CURRENT_DATE) AS days_since_last_order,
    AVG(ot.order_value) AS avg_order_value
FROM silver.customers c
LEFT JOIN order_totals ot ON c.customer_id = ot.customer_id
GROUP BY c.customer_id, c.name;

-- 3. product_performance: per-product sales plus review ratings.
-- Grain: product x supplier. Sales and reviews are aggregated per product
-- before joining to products to avoid N x M multiplication.
CREATE OR REPLACE TABLE gold.product_performance AS
WITH product_sales AS (
    SELECT
        product_id,
        SUM(quantity) AS total_units_sold,
        SUM(price * quantity) AS total_revenue
    FROM silver.order_items
    WHERE NOT is_quarantined
    GROUP BY product_id
),
product_reviews AS (
    SELECT
        product_id,
        AVG(rating) AS avg_rating,
        COUNT(review_text) AS total_reviews
    FROM silver.customer_reviews
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.name AS product_name,
    p.category_id,
    c.name AS category_name,
    supplier_products.supplier_id,
    s.supplier_name,
    sales.total_units_sold,
    sales.total_revenue,
    reviews.avg_rating,
    COALESCE(reviews.total_reviews, 0) AS total_reviews
FROM silver.products p
LEFT JOIN silver.categories c ON p.category_id = c.category_id
LEFT JOIN silver.product_suppliers supplier_products
    ON p.product_id = supplier_products.product_id
LEFT JOIN silver.suppliers s ON supplier_products.supplier_id = s.supplier_id
LEFT JOIN product_sales sales ON p.product_id = sales.product_id
LEFT JOIN product_reviews reviews ON p.product_id = reviews.product_id
WHERE p.is_quarantined = FALSE;

-- 4. supplier_analysis: per-supplier contribution.
-- Grain: supplier. Product revenue attributed to every active supplier link.
CREATE OR REPLACE TABLE gold.supplier_analysis AS
WITH product_sales AS (
    SELECT
        product_id,
        SUM(quantity) AS total_units_sold,
        SUM(price * quantity) AS total_revenue
    FROM silver.order_items
    WHERE NOT is_quarantined
    GROUP BY product_id
)
SELECT
    s.supplier_id,
    s.supplier_name,
    COUNT(DISTINCT supplier_products.product_id) AS total_products_supplied,
    SUM(sales.total_units_sold) AS total_units_sold,
    SUM(sales.total_revenue) AS total_revenue
FROM silver.suppliers s
LEFT JOIN silver.product_suppliers supplier_products
    ON s.supplier_id = supplier_products.supplier_id
LEFT JOIN product_sales sales ON supplier_products.product_id = sales.product_id
WHERE s.is_quarantined = FALSE
GROUP BY 1, 2;

-- 5. customer_reviews_summary: rating summary per product.
-- Grain: product. positive = rating >= 4, negative = rating < 3.
CREATE OR REPLACE TABLE gold.customer_reviews_summary AS
SELECT
    p.product_id,
    p.name AS product_name,
    AVG(cr.rating) AS avg_rating,
    COUNT(cr.review_text) AS total_reviews,
    COUNT(CASE WHEN cr.rating >= 4 THEN 1 END) AS positive_reviews,
    COUNT(CASE WHEN cr.rating < 3 THEN 1 END) AS negative_reviews
FROM silver.products p
LEFT JOIN silver.customer_reviews cr ON p.product_id = cr.product_id
WHERE p.is_quarantined = FALSE
GROUP BY 1, 2;

-- File-vs-database reconciliation (data quality).
-- Answers: which records match, differ, or exist on one side only.

-- 6. product_reconciliation: DB catalog vs ops Excel export.
-- match_status: matched | price_mismatch | file_only | db_only.
-- stock_on_hand exists only in the file.
CREATE OR REPLACE TABLE gold.product_reconciliation AS
SELECT
    COALESCE(p.product_id, f.product_id) AS product_id,
    p.name AS db_name,
    f.name AS file_name,
    p.price AS db_price,
    f.price AS file_price,
    f.stock_on_hand,
    CASE
        WHEN p.product_id IS NULL THEN 'file_only'
        WHEN f.product_id IS NULL THEN 'db_only'
        WHEN p.price IS DISTINCT FROM f.price THEN 'price_mismatch'
        ELSE 'matched'
    END AS match_status
FROM silver.products p
FULL OUTER JOIN silver.product_catalog f ON p.product_id = f.product_id
WHERE COALESCE(p.is_quarantined, FALSE) = FALSE
  AND COALESCE(f.is_quarantined, FALSE) = FALSE;

-- 7. orders_reconciliation: DB orders vs storefront CSV feed.
-- match_status: matched | amount_mismatch | file_only | db_only.
CREATE OR REPLACE TABLE gold.orders_reconciliation AS
SELECT
    COALESCE(o.order_id, f.order_id) AS order_id,
    o.customer_id AS db_customer_id,
    f.customer_id AS file_customer_id,
    o.total_amount AS db_total_amount,
    f.total_amount AS file_total_amount,
    CASE
        WHEN o.order_id IS NULL THEN 'file_only'
        WHEN f.order_id IS NULL THEN 'db_only'
        WHEN o.total_amount IS DISTINCT FROM f.total_amount THEN 'amount_mismatch'
        ELSE 'matched'
    END AS match_status
FROM silver.orders o
FULL OUTER JOIN silver.orders_feed f ON o.order_id = f.order_id
WHERE COALESCE(f.is_quarantined, FALSE) = FALSE;

-- 8. product_catalog_unified: combined DB + file catalog.
-- DB is the primary source; file-only products and stock_on_hand come from the file.
CREATE OR REPLACE TABLE gold.product_catalog_unified AS
SELECT
    COALESCE(p.product_id, f.product_id) AS product_id,
    COALESCE(p.name, f.name) AS name,
    COALESCE(p.category_id, cat_file.category_id) AS category_id,
    COALESCE(cat_db.name, cat_file.name) AS category_name,
    COALESCE(p.price, f.price) AS price,
    f.stock_on_hand,
    CASE WHEN p.product_id IS NULL THEN 'file_only' ELSE 'database' END AS source_system
FROM silver.products p
FULL OUTER JOIN silver.product_catalog f ON p.product_id = f.product_id
LEFT JOIN silver.categories cat_db ON p.category_id = cat_db.category_id
LEFT JOIN silver.categories cat_file
    ON LOWER(TRIM(f.category_name)) = LOWER(TRIM(cat_file.name))
WHERE COALESCE(p.is_quarantined, FALSE) = FALSE
  AND COALESCE(f.is_quarantined, FALSE) = FALSE;
