with SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

list_by_cities as (
    select
        Cities,
        count(email) Count_of_Subs
    from SUBSCRIBERS
    where status = 'Active' and Cities not ilike '%None%' and Cities is not NULL
    group by 1
)

select * from list_by_cities
order by 1