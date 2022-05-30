with opens_quality as (
    select * from {{ref('int2_rf_opens_quality')}}
),

opens_quality_by_growth_channel as (
    select
        Growth_Bucket,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Good' THEN 1 ELSE 0 END) / COUNT(email) Good_Percent_of_Total,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Medium' THEN 1 ELSE 0 END) / COUNT(email) Medium_Percent_of_Total,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Bad' THEN 1 ELSE 0 END)  / COUNT(email) as Bad_Percent_of_Total
    from opens_quality
    group by 1
)

select * from opens_quality_by_growth_channel
order by 1