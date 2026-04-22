{{ config(materialized='table') }}

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),
geo AS (
    SELECT * FROM {{ ref('stg_geolocation') }}
)

SELECT
    s.seller_id,
    s.zip_code,
    s.city,
    s.state,
    g.latitude,
    g.longitude
FROM sellers s
LEFT JOIN geo g ON s.zip_code = g.zip_code