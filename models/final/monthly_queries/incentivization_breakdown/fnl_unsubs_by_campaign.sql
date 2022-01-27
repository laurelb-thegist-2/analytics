with unsubs as (
    select * from {{ref('int1_unsubs')}}
),

unsubs_by_campaign as (
    select
        Unsub_Date,
        sum(CASE WHEN Growth_Channel ilike '%contest%' or Growth_Channel ilike '%coreg%' THEN 1 ELSE 0 END) Incent_Unsubs,
        sum(CASE WHEN Growth_Channel ilike '%contest%' or Growth_Channel ilike '%coreg%' or Growth_Channel is NULL THEN 0 ELSE 1 END) Non_Incent_Unsubs
    from unsubs
    GROUP BY 1
)

select * from unsubs_by_campaign
where Unsub_Date > '2021-12-31'
ORDER BY Unsub_Date desc