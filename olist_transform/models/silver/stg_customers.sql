{{ config(materialized='table') }}

WITH base AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY customer_zip_code_prefix) as rn
    FROM {{ source('olist_bronze', 'customers') }}
)

SELECT 
    customer_id,
    customer_unique_id,
    customer_zip_code_prefix as zip_code,
    UPPER(customer_city) as city,
    customer_state as state
FROM base
WHERE rn = 1