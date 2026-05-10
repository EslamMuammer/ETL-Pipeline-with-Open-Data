{{ config(materialized='view') }}

with raw_sellers as (
    select
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    from {{ source('Olist_Analysis', 'Sellers') }}
),

final_sellers as (
    select
        seller_id,
        seller_zip_code_prefix,
        lower(seller_city) as seller_city,
        upper(seller_state) as seller_state
    from raw_sellers
)

select * from final_sellers