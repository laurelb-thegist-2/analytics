with SUBSCRIBERS as (
    select * from {{ref('int2_rf_opens_quality')}}
),

list_by_sub_quality as (
    select
        Open_Recency_Frequency_Rating,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active'
    group by 1
)

select * from list_by_sub_quality
