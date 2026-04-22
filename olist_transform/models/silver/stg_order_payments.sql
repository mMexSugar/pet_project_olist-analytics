{{ config(materialized='table') }}

SELECT
    order_id,
    payment_sequential AS payment_sequence,
    LOWER(payment_type) AS payment_method,
    payment_installments AS installments,
    payment_value AS payment_amount
FROM {{ source('olist_bronze', 'order_payments') }}