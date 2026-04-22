{{ config(materialized='table') }}

SELECT
    review_id,
    order_id,
    review_score AS score,
    TRIM(review_comment_title) AS comment_title,
    TRIM(review_comment_message) AS comment_message,
    SAFE_CAST(review_creation_date AS TIMESTAMP) AS created_at,
    SAFE_CAST(review_answer_timestamp AS TIMESTAMP) AS answered_at
FROM {{ source('olist_bronze', 'order_reviews') }}