SELECT
    order_id,
    SUM(payment_amount) AS total_payment_amount,
    COUNT(payment_sequence) AS payment_installments_count,
    APPROX_TOP_SUM(payment_method, payment_amount, 1)[OFFSET(0)].value AS main_payment_method
FROM {{ ref('stg_order_payments') }}
GROUP BY 1