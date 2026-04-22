{{ config(materialized='table') }}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
)

SELECT
    product_id,
    category_name,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    (length_cm * height_cm * width_cm) AS volume_cm3
FROM products