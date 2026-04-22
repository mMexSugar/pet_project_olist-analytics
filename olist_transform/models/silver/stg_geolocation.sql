{{ config(materialized='table') }}

WITH raw_geo AS (
    SELECT 
        geolocation_zip_code_prefix AS zip_code,
        geolocation_lat AS latitude,
        geolocation_lng AS longitude,
        INITCAP(geolocation_city) AS city,
        UPPER(geolocation_state) AS state,
        ROW_NUMBER() OVER (
            PARTITION BY geolocation_zip_code_prefix 
            ORDER BY geolocation_lat DESC
        ) as rn
    FROM {{ source('olist_bronze', 'geolocation') }}
)

SELECT
    zip_code,
    latitude,
    longitude,
    city,
    state
FROM raw_geo
WHERE rn = 1