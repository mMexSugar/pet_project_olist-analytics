{{ config(materialized='table') }}

WITH raw_items AS (
    SELECT * FROM {{ source('olist_bronze', 'order_items') }}
)

SELECT
    order_id,
    order_item_id AS item_sequence,
    product_id,
    seller_id,
    shipping_limit_date AS shipping_limit_at,
    price,
    freight_value,
    ROUND(price + freight_value, 2) AS total_item_value
FROM raw_items