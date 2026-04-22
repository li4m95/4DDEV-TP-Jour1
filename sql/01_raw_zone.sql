-- ============================================================
-- RAW ZONE — data_lake.raw_zone
-- Stockage brut en JSONB depuis source_data
-- ============================================================

DROP TABLE IF EXISTS raw_zone.raw_customers;
CREATE TABLE raw_zone.raw_customers (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_geolocation;
CREATE TABLE raw_zone.raw_geolocation (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_order_items;
CREATE TABLE raw_zone.raw_order_items (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_order_payments;
CREATE TABLE raw_zone.raw_order_payments (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_order_reviews;
CREATE TABLE raw_zone.raw_order_reviews (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_orders;
CREATE TABLE raw_zone.raw_orders (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_products;
CREATE TABLE raw_zone.raw_products (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_sellers;
CREATE TABLE raw_zone.raw_sellers (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

DROP TABLE IF EXISTS raw_zone.raw_product_category_translation;
CREATE TABLE raw_zone.raw_product_category_translation (
    raw_id              SERIAL PRIMARY KEY,
    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_table        TEXT,
    payload             JSONB
);

-- ============================================================
-- INSERT depuis source_data
-- ============================================================

INSERT INTO raw_zone.raw_customers (source_table, payload)
SELECT 'customers', row_to_json(t)::jsonb
FROM source_data.customers t;

INSERT INTO raw_zone.raw_geolocation (source_table, payload)
SELECT 'geolocation', row_to_json(t)::jsonb
FROM source_data.geolocation t;

INSERT INTO raw_zone.raw_order_items (source_table, payload)
SELECT 'order_items', row_to_json(t)::jsonb
FROM source_data.order_items t;

INSERT INTO raw_zone.raw_order_payments (source_table, payload)
SELECT 'order_payments', row_to_json(t)::jsonb
FROM source_data.order_payments t;

INSERT INTO raw_zone.raw_order_reviews (source_table, payload)
SELECT 'order_reviews', row_to_json(t)::jsonb
FROM source_data.order_reviews t;

INSERT INTO raw_zone.raw_orders (source_table, payload)
SELECT 'orders', row_to_json(t)::jsonb
FROM source_data.orders t;

INSERT INTO raw_zone.raw_products (source_table, payload)
SELECT 'products', row_to_json(t)::jsonb
FROM source_data.products t;

INSERT INTO raw_zone.raw_sellers (source_table, payload)
SELECT 'sellers', row_to_json(t)::jsonb
FROM source_data.sellers t;

INSERT INTO raw_zone.raw_product_category_translation (source_table, payload)
SELECT 'product_category_translation', row_to_json(t)::jsonb
FROM source_data.product_category_translation t;
