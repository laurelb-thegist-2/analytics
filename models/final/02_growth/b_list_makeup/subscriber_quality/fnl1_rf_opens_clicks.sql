with SUBSCRIBERS as (
    select * from {{ref('int_recency_frequency')}}
),

list_by_sub_quality as (
    select
        Recency_Frequency,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active'
    group by 1
)

select * from list_by_sub_quality
