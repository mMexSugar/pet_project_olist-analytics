{{ config(materialized='table') }}

SELECT
    seller_id,
    seller_zip_code_prefix AS zip_code,
    INITCAP(seller_city) AS city,
    UPPER(seller_state) AS state
FROM {{ source('olist_bronze', 'sellers') }}