SELECT
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    g.latitude,
    g.longitude
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_geolocation') }} g ON c.zip_code = g.zip_code