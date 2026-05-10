{{ config(materialized='view') }}

with raw_products as (
    select
        product_id,
        product_category_name
    from {{ source('Olist_Analysis', 'Products') }}
),

translations as (
    select
        product_category_name,
        product_category_name_english
    from {{ source('Olist_Analysis', 'Product_Category_Name_Translation') }}
),

final_products as (
    select
        p.product_id,
        case
            when p.product_category_name is null 
                then 'Uncategorized (Data Gap)'
            when t.product_category_name_english is null 
                then 'Translation Missing'
            else t.product_category_name_english
        end as product_category
    from raw_products p
    left join translations t 
        on p.product_category_name = t.product_category_name
)

select * from final_products