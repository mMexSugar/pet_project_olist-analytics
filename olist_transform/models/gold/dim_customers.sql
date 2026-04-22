{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Добавим информацию о датах заказов, чтобы найти "последний" адрес
last_orders AS (
    SELECT 
        customer_id,
        MAX(order_purchase_timestamp) as last_order_at
    FROM {{ source('olist_bronze', 'orders') }}
    GROUP BY 1
),

locations AS (
    SELECT * FROM {{ ref('int_customer_locations') }}
),

joined AS (
    SELECT
        c.customer_unique_id,
        c.zip_code,
        c.city,
        c.state,
        l.latitude,
        l.longitude,
        lo.last_order_at,
        -- Ранжируем адреса одного и того же человека по дате покупки
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id 
            ORDER BY lo.last_order_at DESC
        ) as address_rank
    FROM customers c
    LEFT JOIN last_orders lo ON c.customer_id = lo.customer_id
    LEFT JOIN locations l ON c.customer_id = l.customer_id
)

SELECT
    customer_unique_id,
    zip_code,
    city,
    state,
    latitude,
    longitude
FROM joined
WHERE address_rank = 1