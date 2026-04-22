{{ config(
    materialized='table',
    partition_by={
      "field": "purchased_at",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["status", "customer_unique_id"]
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items_summary AS (
    SELECT
        order_id,
        COUNT(item_sequence) AS total_items,
        SUM(price) AS total_items_price,
        SUM(freight_value) AS total_freight,
        SUM(total_item_value) AS total_order_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY 1
),

payments AS (
    SELECT * FROM {{ ref('int_order_payments_aggregated') }}
),

customers AS (
    SELECT 
        customer_id, 
        customer_unique_id 
    FROM {{ ref('stg_customers') }}
)

SELECT
    o.order_id,
    c.customer_unique_id,
    o.status,
    
    o.purchased_at,
    o.approved_at,
    o.delivered_at,
    o.estimated_delivery_at,
    
    TIMESTAMP_DIFF(o.delivered_at, o.purchased_at, DAY) AS actual_delivery_time_days,
    TIMESTAMP_DIFF(o.estimated_delivery_at, o.purchased_at, DAY) AS estimated_delivery_time_days,
    
    COALESCE(oi.total_items, 0) AS total_items_count,
    ROUND(COALESCE(oi.total_items_price, 0), 2) AS total_items_price,
    ROUND(COALESCE(oi.total_freight, 0), 2) AS total_freight_value,
    ROUND(COALESCE(oi.total_order_value, 0), 2) AS total_order_value,
    ROUND(COALESCE(p.total_payment_amount, 0), 2) AS total_payment_value,
    
    p.main_payment_method,
    p.payment_installments_count
    
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_items_summary oi ON o.order_id = oi.order_id
LEFT JOIN payments p ON o.order_id = p.order_id