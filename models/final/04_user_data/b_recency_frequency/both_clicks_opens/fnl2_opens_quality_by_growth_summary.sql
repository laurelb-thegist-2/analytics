with sub_quality as (
    select * from {{ref('int_recency_frequency')}}
),

sub_quality_by_growth_summary as (
    select
        Growth_Summary,
        SUM(CASE WHEN Recency_Frequency = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Recency_Frequency = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
    from sub_quality
    where status = 'Active'
    group by 1
)

select * from sub_quality_by_growth_summary
order by 1