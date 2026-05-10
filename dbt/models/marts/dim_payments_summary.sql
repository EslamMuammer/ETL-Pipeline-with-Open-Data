{{ config(materialized='view') }}

select
    order_id,
    sum(total_payment_value) as grand_total_payment,
    payment_methods_count,
    credit_card_value,
    boleto_value,
    voucher_value,
    debit_card_value
from {{ ref('stg_payments') }}
group by all 