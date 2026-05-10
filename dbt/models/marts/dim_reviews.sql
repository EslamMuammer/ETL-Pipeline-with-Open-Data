{{ config(materialized='view') }}

select
    order_id,
    review_id,
    review_score,
    review_creation_ts, 
    sentiment
from {{ ref('stg_reviews') }}