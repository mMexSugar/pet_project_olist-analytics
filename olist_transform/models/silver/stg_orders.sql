{{ config(materialized='table') }}

SELECT
    order_id,
    customer_id,
    order_status AS status,
    -- Используем SAFE_CAST для надежности, если в Parquet проскочила битая строка
    SAFE_CAST(order_purchase_timestamp AS TIMESTAMP) AS purchased_at,
    SAFE_CAST(order_approved_at AS TIMESTAMP) AS approved_at,
    SAFE_CAST(order_delivered_carrier_date AS TIMESTAMP) AS transferred_to_carrier_at,
    SAFE_CAST(order_delivered_customer_date AS TIMESTAMP) AS delivered_at,
    SAFE_CAST(order_estimated_delivery_date AS TIMESTAMP) AS estimated_delivery_at
FROM {{ source('olist_bronze', 'orders') }}