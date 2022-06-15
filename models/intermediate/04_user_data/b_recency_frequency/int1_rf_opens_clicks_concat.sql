with opens as (
    select * from {{ref('int2_rf_opens_quality')}}
),

clicks as (
    select * from {{ref('int2_rf_clicks_quality')}}
),

concat as (
    select 
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        Growth_Summary,
        Email,
        Status,
        Country,
        Cities,
        Delivered,
        opens.MOST_RECENT_OPEN,
        opens.Opens_Recency_Rating,
        opens.Unique_Opens,
        opens.Unique_Open_Rate,
        opens.Opens_Frequency_Rating,
        clicks.MOST_RECENT_CLICK,
        clicks.Clicks_Recency_Rating,
        clicks.Total_Clicks,
        clicks.Click_Rate,
        clicks.Clicks_Frequency_Rating,
        concat(opens.Open_Recency_Frequency_Rating, clicks.Click_Recency_Frequency_Rating) as Open_Click_Recency_Frequency_Rating
    from opens
    LEFT JOIN clicks using (Growth_Channel, Growth_Int_Bucket, Growth_Bucket, Growth_Summary, Email, Status, Country, Cities, Delivered)
)

select * from concat