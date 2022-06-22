with sub_quality as (
    select * from {{ref('int_recency_frequency')}}
),

sub_quality_by_growth_summary as (
    select
        Growth_Summary,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Good' THEN 1 ELSE 0 END) / COUNT(email) Good_Percent_of_Total,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Medium' THEN 1 ELSE 0 END) / COUNT(email) Medium_Percent_of_Total,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers,
        SUM(CASE WHEN Open_Recency_Frequency_Rating = 'Bad' THEN 1 ELSE 0 END)  / COUNT(email) as Bad_Percent_of_Total
    from sub_quality
    where status = 'Active'
    group by 1
)

select * from sub_quality_by_growth_summary
order by 1