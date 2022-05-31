with SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

list_by_growth_summary as (
    select
        Growth_Summary,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active'
    group by 1
)

select * from list_by_growth_summary
order by 1
