-- ============================================================
-- PROCESSED ZONE — data_lake.processed_zone
-- Extraction JSONB + typage + nettoyage depuis raw_zone
-- ============================================================

DROP TABLE IF EXISTS processed_zone.customers;
CREATE TABLE processed_zone.customers AS
SELECT
    payload->>'customer_id'                  AS customer_id,
    payload->>'customer_unique_id'           AS customer_unique_id,
    TRIM(payload->>'customer_city')          AS customer_city,
    UPPER(TRIM(payload->>'customer_state'))  AS customer_state,
    payload->>'customer_zip_code_prefix'     AS zip_code_prefix,
    ingestion_timestamp
FROM raw_zone.raw_customers
WHERE payload->>'customer_id' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.geolocation;
CREATE TABLE processed_zone.geolocation AS
SELECT
    payload->>'geolocation_zip_code_prefix'         AS zip_code_prefix,
    (payload->>'geolocation_lat')::numeric          AS latitude,
    (payload->>'geolocation_lng')::numeric          AS longitude,
    TRIM(payload->>'geolocation_city')              AS city,
    UPPER(TRIM(payload->>'geolocation_state'))      AS state,
    ingestion_timestamp
FROM raw_zone.raw_geolocation
WHERE payload->>'geolocation_zip_code_prefix' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.order_items;
CREATE TABLE processed_zone.order_items AS
SELECT
    payload->>'order_id'                            AS order_id,
    (payload->>'order_item_id')::integer            AS order_item_id,
    payload->>'product_id'                          AS product_id,
    payload->>'seller_id'                           AS seller_id,
    (payload->>'shipping_limit_date')::timestamp    AS shipping_limit_date,
    (payload->>'price')::numeric                    AS price,
    (payload->>'freight_value')::numeric            AS freight_value,
    ingestion_timestamp
FROM raw_zone.raw_order_items
WHERE payload->>'order_id' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.order_payments;
CREATE TABLE processed_zone.order_payments AS
SELECT
    payload->>'order_id'                            AS order_id,
    (payload->>'payment_sequential')::integer       AS payment_sequential,
    payload->>'payment_type'                        AS payment_type,
    (payload->>'payment_installments')::integer     AS payment_installments,
    (payload->>'payment_value')::numeric            AS payment_value,
    ingestion_timestamp
FROM raw_zone.raw_order_payments
WHERE payload->>'order_id' IS NOT NULL
  AND (payload->>'payment_value')::numeric > 0;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.order_reviews;
CREATE TABLE processed_zone.order_reviews AS
SELECT
    payload->>'review_id'                               AS review_id,
    payload->>'order_id'                                AS order_id,
    (payload->>'review_score')::smallint                AS review_score,
    TRIM(payload->>'review_comment_title')              AS review_comment_title,
    TRIM(payload->>'review_comment_message')            AS review_comment_message,
    (payload->>'review_creation_date')::timestamp       AS review_creation_date,
    (payload->>'review_answer_timestamp')::timestamp    AS review_answer_timestamp,
    ingestion_timestamp
FROM raw_zone.raw_order_reviews
WHERE payload->>'review_id' IS NOT NULL
  AND (payload->>'review_score')::smallint BETWEEN 1 AND 5;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.orders;
CREATE TABLE processed_zone.orders AS
SELECT
    payload->>'order_id'                                        AS order_id,
    payload->>'customer_id'                                     AS customer_id,
    payload->>'order_status'                                    AS order_status,
    (payload->>'order_purchase_timestamp')::timestamp           AS order_purchase_timestamp,
    (payload->>'order_approved_at')::timestamp                  AS order_approved_at,
    (payload->>'order_delivered_carrier_date')::timestamp       AS order_delivered_carrier_date,
    (payload->>'order_delivered_customer_date')::timestamp      AS order_delivered_customer_date,
    (payload->>'order_estimated_delivery_date')::timestamp      AS order_estimated_delivery_date,
    ingestion_timestamp
FROM raw_zone.raw_orders
WHERE payload->>'order_id' IS NOT NULL
  AND payload->>'customer_id' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.products;
CREATE TABLE processed_zone.products AS
SELECT
    payload->>'product_id'                              AS product_id,
    TRIM(payload->>'product_category_name')             AS product_category_name,
    (payload->>'product_name_lenght')::integer          AS product_name_length,
    (payload->>'product_description_lenght')::integer   AS product_description_length,
    (payload->>'product_photos_qty')::integer           AS product_photos_qty,
    (payload->>'product_weight_g')::numeric             AS product_weight_g,
    (payload->>'product_length_cm')::numeric            AS product_length_cm,
    (payload->>'product_height_cm')::numeric            AS product_height_cm,
    (payload->>'product_width_cm')::numeric             AS product_width_cm,
    ingestion_timestamp
FROM raw_zone.raw_products
WHERE payload->>'product_id' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.sellers;
CREATE TABLE processed_zone.sellers AS
SELECT
    payload->>'seller_id'                           AS seller_id,
    payload->>'seller_zip_code_prefix'              AS zip_code_prefix,
    TRIM(payload->>'seller_city')                   AS seller_city,
    UPPER(TRIM(payload->>'seller_state'))           AS seller_state,
    ingestion_timestamp
FROM raw_zone.raw_sellers
WHERE payload->>'seller_id' IS NOT NULL;

-- ------------------------------------------------------------

DROP TABLE IF EXISTS processed_zone.product_category_translation;
CREATE TABLE processed_zone.product_category_translation AS
SELECT
    TRIM(payload->>'product_category_name')             AS product_category_name,
    TRIM(payload->>'product_category_name_english')     AS product_category_name_english,
    ingestion_timestamp
FROM raw_zone.raw_product_category_translation
WHERE payload->>'product_category_name' IS NOT NULL;
