{{ config(materialized='view') }}

select
    order_id,
    order_status,
    order_purchase_ts,
    delivered_customer_ts,
    date_diff('day', order_purchase_ts, delivered_customer_ts) as actual_delivery_days,
    
    case 
        when delivered_customer_ts is null then 'Not Yet Delivered'
        else 'Delivered'  
    end as delivery_status_flag
from {{ ref('stg_orders') }}