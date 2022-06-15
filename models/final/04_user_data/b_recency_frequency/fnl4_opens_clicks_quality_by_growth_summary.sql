with subscriber_quality as (
    select * from {{ref('int2_rf_opens_clicks_quality')}}
),

subscriber_quality_by_growth_channel as (
    select
        Growth_Summary,
        SUM(CASE WHEN Susbscriber_Quality = 'Good' THEN 1 ELSE 0 END) as Good_Subscribers,
        --SUM(CASE WHEN Susbscriber_Quality = 'Good' THEN 1 ELSE 0 END) / COUNT(email) Good_Percent_of_Total,
        SUM(CASE WHEN Susbscriber_Quality = 'Medium' THEN 1 ELSE 0 END) as Medium_Subscribers,
        --SUM(CASE WHEN Susbscriber_Quality = 'Medium' THEN 1 ELSE 0 END) / COUNT(email) Medium_Percent_of_Total,
        SUM(CASE WHEN Susbscriber_Quality = 'Bad' THEN 1 ELSE 0 END) as Bad_Subscribers
        --SUM(CASE WHEN Susbscriber_Quality = 'Bad' THEN 1 ELSE 0 END)  / COUNT(email) as Bad_Percent_of_Total
    from subscriber_quality
    where status = 'Active'
    group by 1
)

select * from subscriber_quality_by_growth_channel
order by 1