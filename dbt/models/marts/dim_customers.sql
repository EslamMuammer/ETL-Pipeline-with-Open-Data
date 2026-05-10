{{ config(materialized='view') }}

select
    customer_id,
    customer_unique_id,
    lower(customer_city) as customer_city,
    upper(customer_state) as customer_state
from {{ ref('stg_customers') }}