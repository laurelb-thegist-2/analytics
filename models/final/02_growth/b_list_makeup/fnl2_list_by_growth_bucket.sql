with SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

list_by_growth_bucket as (
    select
        Growth_Bucket,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active'
    group by 1
)

select * from list_by_growth_bucket
order by 1
