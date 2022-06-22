with opens_quality as (
    select * from {{ref('int_recency_frequency')}}
),

opens_quality_by_growth_bucket as (
    select
        Growth_Bucket,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
    from opens_quality
    where status = 'Active'
    group by 1
)

select * from opens_quality_by_growth_bucket
order by 1