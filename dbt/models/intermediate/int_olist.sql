{{ config(materialized='view') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

order_payments as (
    select 
        order_id,
        sum(total_payment_value) as total_order_value
    from {{ ref('stg_payments') }}
    group by 1
),

order_items as (
    select 
        order_id,
        count(product_id) as total_items_count,
        count(distinct product_id) as distinct_products_count
    from {{ ref('stg_order_items') }}
    group by 1
),

final_summary as (
    select
        -- KPIs المطلوبة
        count(distinct o.order_id) as total_orders,
        count(distinct o.customer_id) as total_customers,
        sum(i.distinct_products_count) as total_distinct_products_sold,
        sum(p.total_order_value) as total_revenue,
        min(o.order_purchase_ts) as first_order_timestamp,
        max(o.order_purchase_ts) as last_order_timestamp
    from orders o
    left join order_payments p on o.order_id = p.order_id
    left join order_items i on o.order_id = i.order_id
)

select * from final_summary