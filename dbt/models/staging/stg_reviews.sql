{{ config(materialized='view') }}

with raw_reviews as (
    select
        review_id,
        order_id,
        review_score,
        cast(review_creation_date as timestamp) as review_creation_ts,
        cast(review_answer_timestamp as timestamp) as review_answer_ts,
        row_number() over(
            partition by order_id 
            order by review_creation_date desc
        ) as latest_review_rn
    from {{ source('Olist_Analysis', 'Order_Reviews') }}
),

final_reviews as (
    select
        order_id,
        review_id,
        review_score,
        review_creation_ts,
        
        case 
            when review_score = 1 then 'Very Bad'
            when review_score = 2 then 'Bad'
            when review_score = 3 then 'Neutral'
            when review_score = 4 then 'Good'
            when review_score = 5 then 'Excellent'
            else 'No Review'
        end as score_label,
        
        case 
            when review_score >= 4 then 'Positive'
            when review_score = 3 then 'Neutral'
            when review_score <= 2 then 'Negative'
            else 'No Review'
        end as sentiment

    from raw_reviews
    where latest_review_rn = 1
)

select * from final_reviews