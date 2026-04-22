SELECT
    o.order_id,
    o.status,
    o.purchased_at,
    o.delivered_at,
    cl.city AS customer_city,
    cl.state AS customer_state,
    cl.latitude,
    cl.longitude,
    TIMESTAMP_DIFF(o.delivered_at, o.purchased_at, DAY) AS delivery_time_days
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('int_customer_locations') }} cl ON o.customer_id = cl.customer_id