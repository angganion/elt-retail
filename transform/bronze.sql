-- Bronze: raw views over landing Parquet. No transformation.
-- Each view maps to one landing folder; the '*.parquet' glob picks up all
-- date files in that folder. Archive folders are excluded by location.

CREATE SCHEMA IF NOT EXISTS bronze;

-- Column lists mirror the landing files; types are inferred from Parquet.
-- To change columns or paths, edit here only; upper layers read these views.

-- orders: retailer orders.
CREATE OR REPLACE VIEW bronze.orders AS
SELECT order_id, customer_id, order_date, total_amount, updated_at
FROM read_parquet('data/retail-datalake/landing/retailer-db/orders/*.parquet');

-- customers: retailer customer profiles.
CREATE OR REPLACE VIEW bronze.customers AS
SELECT customer_id, name, email, updated_at
FROM read_parquet('data/retail-datalake/landing/retailer-db/customers/*.parquet');

-- products: retailer product catalog.
CREATE OR REPLACE VIEW bronze.products AS
SELECT product_id, name, category_id, price, updated_at
FROM read_parquet('data/retail-datalake/landing/retailer-db/products/*.parquet');

-- categories: retailer product categories.
CREATE OR REPLACE VIEW bronze.categories AS
SELECT category_id, name, updated_at
FROM read_parquet('data/retail-datalake/landing/retailer-db/categories/*.parquet');

-- order_items: line items per order.
CREATE OR REPLACE VIEW bronze.order_items AS
SELECT order_item_id, order_id, product_id, quantity, price, updated_at
FROM read_parquet('data/retail-datalake/landing/retailer-db/order_items/*.parquet');

-- suppliers: supplier master data.
CREATE OR REPLACE VIEW bronze.suppliers AS
SELECT supplier_id, supplier_name, contact_name, phone, email, address, city, country, created_at
FROM read_parquet('data/retail-datalake/landing/supplier-db/suppliers/*.parquet');

-- product_suppliers: supplier-product relationships with supply prices.
CREATE OR REPLACE VIEW bronze.product_suppliers AS
SELECT supplier_id, product_id, supply_price, last_updated
FROM read_parquet('data/retail-datalake/landing/supplier-db/product_suppliers/*.parquet');

-- customer_reviews: customer reviews from the API source.
CREATE OR REPLACE VIEW bronze.customer_reviews AS
SELECT id, customer_id, product_id, rating, review_text, review_date
FROM read_parquet('data/retail-datalake/landing/customer_reviews/*.parquet');

-- File sources, ingested by ingestion/file_sources_to_landing.py.
-- Content partly overlaps the DB tables above in different formats;
-- reconciliation lives in the gold layer.

-- product_catalog: ops team product export (Excel).
CREATE OR REPLACE VIEW bronze.product_catalog AS
SELECT product_id, name, category_name, price, stock_on_hand, updated_at
FROM read_parquet('data/retail-datalake/landing/file-sources/product_catalog/*.parquet');

-- orders_feed: storefront order export (CSV).
CREATE OR REPLACE VIEW bronze.orders_feed AS
SELECT order_id, customer_id, order_date, total_amount
FROM read_parquet('data/retail-datalake/landing/file-sources/orders_feed/*.parquet');
