-- ============================================================
-- Données enrichies, jointurées, prêtes pour l'analytics
-- ============================================================

-- ------------------------------------------------------------
-- 1. orders_enriched
--    orders + customers + paiements agrégés
-- ------------------------------------------------------------
DROP TABLE IF EXISTS curated_zone.orders_enriched;
CREATE TABLE curated_zone.orders_enriched AS
SELECT
    o.order_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- délai de livraison en jours
    EXTRACT(DAY FROM (
        o.order_delivered_customer_date - o.order_purchase_timestamp
    ))::integer                                         AS delivery_days,

    -- retard (positif = en retard, négatif = en avance)
    EXTRACT(DAY FROM (
        o.order_delivered_customer_date - o.order_estimated_delivery_date
    ))::integer                                         AS delivery_delay_days,

    -- customer
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.zip_code_prefix,

    -- paiements agrégés
    p.total_amount,
    p.payment_types,
    p.installments_max

FROM processed_zone.orders o
JOIN processed_zone.customers c
    ON o.customer_id = c.customer_id
LEFT JOIN (
    SELECT
        order_id,
        SUM(payment_value)                          AS total_amount,
        STRING_AGG(DISTINCT payment_type, ', ')     AS payment_types,
        MAX(payment_installments)                   AS installments_max
    FROM processed_zone.order_payments
    GROUP BY order_id
) p ON o.order_id = p.order_id;

-- ------------------------------------------------------------
-- 2. order_items_enriched
--    order_items + products + catégorie traduite + sellers
-- ------------------------------------------------------------
DROP TABLE IF EXISTS curated_zone.order_items_enriched;
CREATE TABLE curated_zone.order_items_enriched AS
SELECT
    oi.order_id,
    oi.order_item_id,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value)                   AS total_item_amount,

    -- product
    oi.product_id,
    pr.product_category_name,
    COALESCE(tr.product_category_name_english,
             pr.product_category_name)              AS product_category_en,
    pr.product_weight_g,
    pr.product_length_cm,
    pr.product_height_cm,
    pr.product_width_cm,

    -- seller
    oi.seller_id,
    s.seller_city,
    s.seller_state

FROM processed_zone.order_items oi
LEFT JOIN processed_zone.products pr
    ON oi.product_id = pr.product_id
LEFT JOIN processed_zone.product_category_translation tr
    ON pr.product_category_name = tr.product_category_name
LEFT JOIN processed_zone.sellers s
    ON oi.seller_id = s.seller_id;

-- ------------------------------------------------------------
-- 3. customer_summary
--    customers enrichis avec géolocalisation moyenne
-- ------------------------------------------------------------
DROP TABLE IF EXISTS curated_zone.customer_summary;
CREATE TABLE curated_zone.customer_summary AS
SELECT
    c.customer_id,
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    c.zip_code_prefix,

    -- géolocalisation moyenne du zip code
    g.avg_lat,
    g.avg_lng,

    -- historique commandes
    COUNT(DISTINCT o.order_id)                      AS total_orders,
    SUM(p.total_amount)                             AS total_spent,
    AVG(p.total_amount)                             AS avg_order_value,
    MIN(o.order_purchase_timestamp)                 AS first_order_date,
    MAX(o.order_purchase_timestamp)                 AS last_order_date

FROM processed_zone.customers c
LEFT JOIN (
    SELECT
        zip_code_prefix,
        AVG(latitude)   AS avg_lat,
        AVG(longitude)  AS avg_lng
    FROM processed_zone.geolocation
    GROUP BY zip_code_prefix
) g ON c.zip_code_prefix = g.zip_code_prefix
LEFT JOIN processed_zone.orders o
    ON c.customer_id = o.customer_id
LEFT JOIN (
    SELECT order_id, SUM(payment_value) AS total_amount
    FROM processed_zone.order_payments
    GROUP BY order_id
) p ON o.order_id = p.order_id
GROUP BY
    c.customer_id, c.customer_unique_id,
    c.customer_city, c.customer_state,
    c.zip_code_prefix, g.avg_lat, g.avg_lng;

-- ------------------------------------------------------------
-- 4. delivery_performance
--    métriques de livraison par commande
-- ------------------------------------------------------------
DROP TABLE IF EXISTS curated_zone.delivery_performance;
CREATE TABLE curated_zone.delivery_performance AS
SELECT
    o.order_id,
    o.order_status,
    o.customer_id,
    c.customer_state,

    o.order_purchase_timestamp,
    o.order_approved_at,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,

    -- délais en heures
    EXTRACT(EPOCH FROM (
        o.order_approved_at - o.order_purchase_timestamp
    )) / 3600                                           AS approval_hours,

    EXTRACT(EPOCH FROM (
        o.order_delivered_carrier_date - o.order_approved_at
    )) / 3600                                           AS handling_hours,

    EXTRACT(EPOCH FROM (
        o.order_delivered_customer_date - o.order_delivered_carrier_date
    )) / 3600                                           AS shipping_hours,

    -- délai total en jours
    EXTRACT(DAY FROM (
        o.order_delivered_customer_date - o.order_purchase_timestamp
    ))::integer                                         AS total_delivery_days,

    -- on time ? (TRUE = livré avant ou à la date estimée)
    CASE
        WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date
        THEN TRUE ELSE FALSE
    END                                                 AS delivered_on_time,

    -- score review si disponible
    r.review_score,
    r.review_creation_date

FROM processed_zone.orders o
JOIN processed_zone.customers c
    ON o.customer_id = c.customer_id
LEFT JOIN processed_zone.order_reviews r
    ON o.order_id = r.order_id
WHERE o.order_status = 'delivered';
