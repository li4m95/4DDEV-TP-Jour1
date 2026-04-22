-- ============================================================
-- DATA WAREHOUSE — star schema
-- ETL depuis curated_zone vers data_warehouse
-- ============================================================

-- ------------------------------------------------------------
-- DIMENSIONS
-- ------------------------------------------------------------

-- dim_date
DROP TABLE IF EXISTS data_warehouse.dim_date;
CREATE TABLE data_warehouse.dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  TEXT NOT NULL,
    week        SMALLINT NOT NULL,
    day         SMALLINT NOT NULL,
    day_name    TEXT NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

INSERT INTO data_warehouse.dim_date (
    full_date, year, quarter, month, month_name,
    week, day, day_name, is_weekend
)
SELECT DISTINCT
    order_purchase_timestamp::date              AS full_date,
    EXTRACT(YEAR    FROM order_purchase_timestamp)::smallint AS year,
    EXTRACT(QUARTER FROM order_purchase_timestamp)::smallint AS quarter,
    EXTRACT(MONTH   FROM order_purchase_timestamp)::smallint AS month,
    TO_CHAR(order_purchase_timestamp, 'Month')  AS month_name,
    EXTRACT(WEEK    FROM order_purchase_timestamp)::smallint AS week,
    EXTRACT(DAY     FROM order_purchase_timestamp)::smallint AS day,
    TO_CHAR(order_purchase_timestamp, 'Day')    AS day_name,
    EXTRACT(ISODOW  FROM order_purchase_timestamp) IN (6, 7) AS is_weekend
FROM curated_zone.orders_enriched
WHERE order_purchase_timestamp IS NOT NULL
ORDER BY full_date;

-- ------------------------------------------------------------

-- dim_customer
DROP TABLE IF EXISTS data_warehouse.dim_customer;
CREATE TABLE data_warehouse.dim_customer (
    customer_sk     SERIAL PRIMARY KEY,
    customer_id     TEXT NOT NULL UNIQUE,
    customer_city   TEXT,
    customer_state  TEXT,
    zip_code_prefix TEXT
);

INSERT INTO data_warehouse.dim_customer (
    customer_id, customer_city, customer_state, zip_code_prefix
)
SELECT DISTINCT
    customer_id,
    customer_city,
    customer_state,
    zip_code_prefix
FROM curated_zone.orders_enriched;

-- ------------------------------------------------------------

-- dim_product
DROP TABLE IF EXISTS data_warehouse.dim_product;
CREATE TABLE data_warehouse.dim_product (
    product_sk              SERIAL PRIMARY KEY,
    product_id              TEXT NOT NULL UNIQUE,
    product_category_name   TEXT,
    product_category_en     TEXT,
    product_weight_g        NUMERIC,
    product_length_cm       NUMERIC,
    product_height_cm       NUMERIC,
    product_width_cm        NUMERIC
);

INSERT INTO data_warehouse.dim_product (
    product_id, product_category_name, product_category_en,
    product_weight_g, product_length_cm, product_height_cm, product_width_cm
)
SELECT DISTINCT
    product_id,
    product_category_name,
    product_category_en,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM curated_zone.order_items_enriched
WHERE product_id IS NOT NULL;

-- ------------------------------------------------------------

-- dim_seller
DROP TABLE IF EXISTS data_warehouse.dim_seller;
CREATE TABLE data_warehouse.dim_seller (
    seller_sk       SERIAL PRIMARY KEY,
    seller_id       TEXT NOT NULL UNIQUE,
    seller_city     TEXT,
    seller_state    TEXT
);

INSERT INTO data_warehouse.dim_seller (
    seller_id, seller_city, seller_state
)
SELECT DISTINCT
    seller_id,
    seller_city,
    seller_state
FROM curated_zone.order_items_enriched
WHERE seller_id IS NOT NULL;

-- ------------------------------------------------------------
-- FAIT
-- ------------------------------------------------------------

-- fact_sales (une ligne par item vendu)
DROP TABLE IF EXISTS data_warehouse.fact_sales;
CREATE TABLE data_warehouse.fact_sales (
    sale_id         SERIAL PRIMARY KEY,

    -- clés étrangères
    order_id        TEXT NOT NULL,
    order_item_id   INTEGER NOT NULL,
    date_id         INTEGER REFERENCES data_warehouse.dim_date(date_id),
    customer_sk     INTEGER REFERENCES data_warehouse.dim_customer(customer_sk),
    product_sk      INTEGER REFERENCES data_warehouse.dim_product(product_sk),
    seller_sk       INTEGER REFERENCES data_warehouse.dim_seller(seller_sk),

    -- mesures
    price               NUMERIC,
    freight_value       NUMERIC,
    total_item_amount   NUMERIC,
    payment_total       NUMERIC,
    delivery_days       INTEGER,
    delivery_delay_days INTEGER,
    review_score        SMALLINT,
    order_status        TEXT
);

INSERT INTO data_warehouse.fact_sales (
    order_id, order_item_id,
    date_id, customer_sk, product_sk, seller_sk,
    price, freight_value, total_item_amount, payment_total,
    delivery_days, delivery_delay_days, review_score, order_status
)
SELECT
    oi.order_id,
    oi.order_item_id,

    dd.date_id,
    dc.customer_sk,
    dp.product_sk,
    ds.seller_sk,

    oi.price,
    oi.freight_value,
    oi.total_item_amount,
    oe.total_amount          AS payment_total,
    oe.delivery_days,
    oe.delivery_delay_days,
    r.review_score,
    oe.order_status

FROM curated_zone.order_items_enriched oi
JOIN curated_zone.orders_enriched oe
    ON oi.order_id = oe.order_id
JOIN data_warehouse.dim_date dd
    ON oe.order_purchase_timestamp::date = dd.full_date
JOIN data_warehouse.dim_customer dc
    ON oe.customer_id = dc.customer_id
LEFT JOIN data_warehouse.dim_product dp
    ON oi.product_id = dp.product_id
LEFT JOIN data_warehouse.dim_seller ds
    ON oi.seller_id = ds.seller_id
LEFT JOIN processed_zone.order_reviews r
    ON oi.order_id = r.order_id;
