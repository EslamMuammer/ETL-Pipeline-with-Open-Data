{{ config(materialized='view') }}

with raw_orders as (
    select
        order_id,
        customer_id,
        lower(order_status) as order_status,
        cast(order_purchase_timestamp as timestamp) as order_purchase_ts,
        cast(order_approved_at as timestamp) as order_approved_ts,
        cast(order_delivered_carrier_date as timestamp) as delivered_carrier_ts,
        cast(order_delivered_customer_date as timestamp) as delivered_customer_ts,
        cast(order_estimated_delivery_date as timestamp) as estimated_delivery_ts
    
    from {{ source('Olist_Analysis', 'Orders') }}
),

final as (
    select
        *,
        date_diff('day', order_purchase_ts, delivered_customer_ts) as actual_delivery_days,
        date_diff('day', order_purchase_ts, estimated_delivery_ts) as estimated_delivery_days,
        date_diff('day', estimated_delivery_ts, delivered_customer_ts) as delivery_deviation_days,
        
        case 
            when delivered_customer_ts is null then 'not_delivered'
            when delivered_customer_ts <= estimated_delivery_ts then 'on_time'
            else 'late'
        end as delivery_flag
    from raw_orders
)

select * from final
