{{ config(materialized='view') }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

final as (
    select 
        oi.order_id,
        oi.product_id,
        o.order_purchase_ts,
        o.order_approved_ts, 
        oi.price,
        oi.freight_value,
        (oi.price + oi.freight_value) as item_total
    from order_items oi
    inner join orders o on oi.order_id = o.order_id
)

select * from final