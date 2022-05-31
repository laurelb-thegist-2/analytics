with SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

list_by_country as (
    select
        coalesce(Country, 'US') as Country,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active'
    group by 1
)

select * from list_by_country
order by 1