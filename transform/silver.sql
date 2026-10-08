-- Silver: cleaned data with change history (SCD Type 2).
-- SCD tables use two MERGE statements each: one closes changed active versions,
-- one inserts the new versions. A single MERGE cannot do both because a matched
-- (closed) row would never be re-inserted.
-- History is stored in *_history tables and exposed to gold through active-only
-- views (silver.customers etc.), which intentionally have no is_active column.
-- Gold must reference the base views; *_history is reserved for as-of/audit use
-- (enforced by tests/test_scd_contract.py).
-- order_items is a transactional fact (TRUNCATE + INSERT), not an SCD dimension.
-- is_quarantined marks rows with NULL required columns; only customers_history
-- carries it among the SCD tables.

CREATE SCHEMA IF NOT EXISTS silver;

-- Drop legacy base tables so the active-only views can take their names.
DROP TABLE IF EXISTS silver.customers;
DROP TABLE IF EXISTS silver.orders;
DROP TABLE IF EXISTS silver.product_suppliers;
DROP TABLE IF EXISTS silver.customer_reviews;

-- Stage bronze sources to temp tables first so both MERGE statements in each
-- SCD block read the same snapshot. SELECT DISTINCT also drops source dupes.
CREATE OR REPLACE TEMP TABLE stage_customers AS
SELECT DISTINCT * FROM bronze.customers;

CREATE OR REPLACE TEMP TABLE stage_orders AS
SELECT DISTINCT * FROM bronze.orders;

CREATE OR REPLACE TEMP TABLE stage_product_suppliers AS
SELECT DISTINCT * FROM bronze.product_suppliers;

CREATE OR REPLACE TEMP TABLE stage_customer_reviews AS
SELECT DISTINCT * FROM bronze.customer_reviews;

-- 1. customers (SCD Type 2). Tracked columns: name, email, updated_at.
-- History: customers_history. Gold reads the active-only view below.
CREATE TABLE IF NOT EXISTS silver.customers_history (
    customer_id BIGINT,
    name VARCHAR,
    email VARCHAR,
    updated_at TIMESTAMP,
    is_quarantined BOOLEAN,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN
);

-- Close active versions that changed upstream.
MERGE INTO silver.customers_history AS target
USING stage_customers AS source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN MATCHED AND (target.name != source.name OR target.email != source.email OR target.updated_at != source.updated_at)
THEN UPDATE SET is_active = FALSE, effective_end_date = CURRENT_TIMESTAMP;

-- Insert new active versions. is_quarantined when customer_id, email, or name is NULL.
MERGE INTO silver.customers_history AS target
USING (
    SELECT
        *,
        CASE WHEN customer_id IS NULL OR email IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined,
        CURRENT_TIMESTAMP AS effective_start_date,
        CURRENT_TIMESTAMP AS effective_end_date,
        TRUE AS is_active
    FROM stage_customers
) AS source
ON target.customer_id = source.customer_id AND target.is_active = TRUE
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, updated_at, is_quarantined, effective_start_date, effective_end_date, is_active)
    VALUES (source.customer_id, source.name, source.email, source.updated_at, source.is_quarantined,
            source.effective_start_date, source.effective_end_date, source.is_active);

CREATE OR REPLACE VIEW silver.customers AS
SELECT customer_id, name, email, updated_at, is_quarantined,
       effective_start_date, effective_end_date
FROM silver.customers_history
WHERE is_active;

-- 2. orders (SCD Type 2). Tracked columns: customer_id, order_date, total_amount, updated_at.
-- History: orders_history. Gold reads the active-only view below.
CREATE TABLE IF NOT EXISTS silver.orders_history (
    order_id BIGINT,
    customer_id BIGINT,
    order_date TIMESTAMP,
    total_amount DECIMAL,
    updated_at TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN
);

MERGE INTO silver.orders_history AS target
USING stage_orders AS source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN MATCHED AND (target.customer_id != source.customer_id OR target.order_date != source.order_date
                  OR target.total_amount != source.total_amount OR target.updated_at != source.updated_at)
THEN UPDATE SET is_active = FALSE, effective_end_date = CURRENT_TIMESTAMP;

MERGE INTO silver.orders_history AS target
USING (
    SELECT
        *,
        CURRENT_TIMESTAMP AS effective_start_date,
        CURRENT_TIMESTAMP AS effective_end_date,
        TRUE AS is_active
    FROM stage_orders
) AS source
ON target.order_id = source.order_id AND target.is_active = TRUE
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, order_date, total_amount, updated_at, effective_start_date, effective_end_date, is_active)
    VALUES (source.order_id, source.customer_id, source.order_date, source.total_amount, source.updated_at,
            source.effective_start_date, source.effective_end_date, source.is_active);

CREATE OR REPLACE VIEW silver.orders AS
SELECT order_id, customer_id, order_date, total_amount, updated_at,
       effective_start_date, effective_end_date
FROM silver.orders_history
WHERE is_active;

-- 3. order_items (TRUNCATE + INSERT, not SCD2). Transactional fact.
CREATE TABLE IF NOT EXISTS silver.order_items (
    order_item_id BIGINT,
    order_id BIGINT,
    product_id BIGINT,
    quantity BIGINT,
    price DECIMAL,
    updated_at TIMESTAMP,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.order_items;

INSERT INTO silver.order_items
SELECT
    *,
    CASE WHEN order_item_id IS NULL OR order_id IS NULL OR product_id IS NULL
          OR quantity IS NULL OR price IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.order_items;

-- 4. categories (TRUNCATE + INSERT). Small, slowly changing table.
CREATE TABLE IF NOT EXISTS silver.categories (
    category_id BIGINT,
    name VARCHAR,
    updated_at TIMESTAMP,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.categories;

-- is_quarantined when category_id or name is NULL.
INSERT INTO silver.categories
SELECT
    *,
    CASE WHEN category_id IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.categories;

-- 5. products (TRUNCATE + INSERT).
CREATE TABLE IF NOT EXISTS silver.products (
    product_id BIGINT,
    name VARCHAR,
    category_id BIGINT,
    price DECIMAL,
    updated_at TIMESTAMP,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.products;

-- is_quarantined when category_id or name is NULL.
INSERT INTO silver.products
SELECT
    *,
    CASE WHEN category_id IS NULL OR name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.products;

-- 6. product_suppliers (SCD Type 2, composite key supplier_id + product_id).
-- Tracked columns: supply_price, last_updated.
-- History: product_suppliers_history. Gold reads the active-only view below.
CREATE TABLE IF NOT EXISTS silver.product_suppliers_history (
    supplier_id BIGINT,
    product_id BIGINT,
    supply_price DECIMAL,
    last_updated TIMESTAMP,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN
);

MERGE INTO silver.product_suppliers_history AS target
USING stage_product_suppliers AS source
ON target.supplier_id = source.supplier_id AND target.product_id = source.product_id AND target.is_active = TRUE
WHEN MATCHED AND (target.supply_price != source.supply_price OR target.last_updated != source.last_updated)
THEN UPDATE SET is_active = FALSE, effective_end_date = CURRENT_TIMESTAMP;

MERGE INTO silver.product_suppliers_history AS target
USING (
    SELECT
        *,
        CURRENT_TIMESTAMP AS effective_start_date,
        CURRENT_TIMESTAMP AS effective_end_date,
        TRUE AS is_active
    FROM stage_product_suppliers
) AS source
ON target.supplier_id = source.supplier_id AND target.product_id = source.product_id AND target.is_active = TRUE
WHEN NOT MATCHED THEN
    INSERT (supplier_id, product_id, supply_price, last_updated, effective_start_date, effective_end_date, is_active)
    VALUES (source.supplier_id, source.product_id, source.supply_price, source.last_updated,
            source.effective_start_date, source.effective_end_date, source.is_active);

CREATE OR REPLACE VIEW silver.product_suppliers AS
SELECT supplier_id, product_id, supply_price, last_updated,
       effective_start_date, effective_end_date
FROM silver.product_suppliers_history
WHERE is_active;

-- 7. suppliers (TRUNCATE + INSERT).
CREATE TABLE IF NOT EXISTS silver.suppliers (
    supplier_id BIGINT,
    supplier_name VARCHAR,
    contact_name VARCHAR,
    phone VARCHAR,
    email VARCHAR,
    address VARCHAR,
    city VARCHAR,
    country VARCHAR,
    created_at TIMESTAMP,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.suppliers;

-- is_quarantined when supplier_id or supplier_name is NULL.
INSERT INTO silver.suppliers
SELECT
    *,
    CASE WHEN supplier_id IS NULL OR supplier_name IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.suppliers;

-- 8. customer_reviews (SCD Type 2, API key id).
-- Tracked columns: customer_id, product_id, rating, review_text, review_date.
-- History: customer_reviews_history. Gold reads the active-only view below.
CREATE TABLE IF NOT EXISTS silver.customer_reviews_history (
    id VARCHAR,
    customer_id BIGINT,
    product_id BIGINT,
    rating BIGINT,
    review_text VARCHAR,
    review_date VARCHAR,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_active BOOLEAN
);

MERGE INTO silver.customer_reviews_history AS target
USING stage_customer_reviews AS source
ON target.id = source.id AND target.is_active = TRUE
WHEN MATCHED AND (target.customer_id != source.customer_id OR target.product_id != source.product_id
                  OR target.rating != source.rating OR target.review_text != source.review_text
                  OR target.review_date != source.review_date)
THEN UPDATE SET is_active = FALSE, effective_end_date = CURRENT_TIMESTAMP;

MERGE INTO silver.customer_reviews_history AS target
USING (
    SELECT
        *,
        CURRENT_TIMESTAMP AS effective_start_date,
        CURRENT_TIMESTAMP AS effective_end_date,
        TRUE AS is_active
    FROM stage_customer_reviews
) AS source
ON target.id = source.id AND target.is_active = TRUE
WHEN NOT MATCHED THEN
    INSERT (id, customer_id, product_id, rating, review_text, review_date, effective_start_date, effective_end_date, is_active)
    VALUES (source.id, source.customer_id, source.product_id, source.rating, source.review_text, source.review_date,
            source.effective_start_date, source.effective_end_date, source.is_active);

CREATE OR REPLACE VIEW silver.customer_reviews AS
SELECT id, customer_id, product_id, rating, review_text, review_date,
       effective_start_date, effective_end_date
FROM silver.customer_reviews_history
WHERE is_active;

-- 9. product_catalog (TRUNCATE + INSERT, Excel file source, full snapshots).
-- Category arrives as a name; mapping to category_id happens in gold.
-- is_quarantined when product_id, name, or price is NULL.
CREATE TABLE IF NOT EXISTS silver.product_catalog (
    product_id BIGINT,
    name VARCHAR,
    category_name VARCHAR,
    price DECIMAL,
    stock_on_hand BIGINT,
    updated_at DATE,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.product_catalog;

INSERT INTO silver.product_catalog
SELECT
    *,
    CASE WHEN product_id IS NULL OR name IS NULL OR price IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.product_catalog;

-- 10. orders_feed (TRUNCATE + INSERT, CSV file source, full snapshots).
-- is_quarantined when order_id or total_amount is NULL.
CREATE TABLE IF NOT EXISTS silver.orders_feed (
    order_id BIGINT,
    customer_id BIGINT,
    order_date DATE,
    total_amount DECIMAL,
    is_quarantined BOOLEAN
);

TRUNCATE TABLE silver.orders_feed;

INSERT INTO silver.orders_feed
SELECT
    *,
    CASE WHEN order_id IS NULL OR total_amount IS NULL THEN TRUE ELSE FALSE END AS is_quarantined
FROM bronze.orders_feed;
