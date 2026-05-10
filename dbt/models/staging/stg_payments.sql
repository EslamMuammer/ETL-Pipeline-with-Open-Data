{{ config(materialized='view') }}

with raw_payments as (
    select
        order_id,
        payment_sequential,
        case 
            when payment_type = 'not_defined' then 'other'
            else payment_type 
        end as payment_type,
        payment_installments,
        cast(payment_value as decimal(10,2)) as payment_value
    from {{ source('Olist_Analysis', 'Order_Payments') }}
    where payment_value > 0
),

order_summary as (
    select
        order_id,
        sum(payment_value) as total_payment_value,
        count(distinct payment_type) as payment_methods_count,
        max(payment_installments) as max_installments,
        
        sum(case when payment_type = 'credit_card' then payment_value else 0 end) as credit_card_value,
        sum(case when payment_type = 'boleto' then payment_value else 0 end) as boleto_value,
        sum(case when payment_type = 'voucher' then payment_value else 0 end) as voucher_value,
        sum(case when payment_type = 'debit_card' then payment_value else 0 end) as debit_card_value
        
    from raw_payments
    group by order_id
),

dominant_payment as (
    select 
        order_id, 
        payment_type as dominant_payment_type
    from (
        select 
            order_id, 
            payment_type,
            row_number() over(partition by order_id order by payment_value desc) as rn
        from raw_payments
    )
    where rn = 1
)

select
    s.*,
    d.dominant_payment_type
from order_summary s
left join dominant_payment d 
    on s.order_id = d.order_id