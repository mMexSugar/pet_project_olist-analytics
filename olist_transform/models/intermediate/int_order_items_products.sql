WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),
products AS (
    SELECT * FROM {{ ref('stg_products') }}
)
SELECT
    i.order_id,
    i.item_sequence,
    i.product_id,
    i.seller_id,
    i.price,
    i.freight_value,
    p.category_name,
    p.weight_g
FROM items i
LEFT JOIN products p ON i.product_id = p.product_id