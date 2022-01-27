with unsubs as (
    select * from {{ref('int1_unsubs')}}
),

monthly_unsubs as (
    select
        date_trunc('month', Unsub_Date) as Month,
        sum(CASE WHEN Growth_Channel ilike '%contest%' or Growth_Channel ilike '%coreg%' THEN 1 ELSE 0 END) Incent_Unsubs,
        sum(CASE WHEN Growth_Channel ilike '%contest%' or Growth_Channel ilike '%coreg%' or Growth_Channel is NULL THEN 0 ELSE 1 END) Non_Incent_Unsubs
    from unsubs
    GROUP BY 1
)

select * from monthly_unsubs
where month > '2021-12-31'
ORDER BY Month desc