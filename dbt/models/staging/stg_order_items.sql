{{ config(materialized='view') }}

select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    price,
    freight_value,
    (price + freight_value) as item_total,
    cast(shipping_limit_date as timestamp) as shipping_limit_ts
from {{ source('Olist_Analysis', 'Order_Items') }}