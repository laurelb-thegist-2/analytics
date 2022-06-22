with opens_quality as (
    select * from {{ref('int1_opens_recency_frequency')}}
),

opens_quality_by_growth_summary as (
    select
        Growth_Summary,
        SUM(CASE WHEN Recency_Frequency = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
    from opens_quality
    where status = 'Active'
    group by 1
)

select * from opens_quality_by_growth_summary
order by 1