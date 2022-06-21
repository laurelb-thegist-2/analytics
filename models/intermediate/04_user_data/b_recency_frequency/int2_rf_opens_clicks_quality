with concat as (
    select * from {{ref('int1_rf_opens_clicks_concat')}}
),

rf_analysis as (
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
    MOST_RECENT_OPEN,
    Unique_Opens,
    UNIQUE_OPEN_RATE,
    MOST_RECENT_CLICK,
    Total_Clicks,
    CLICK_RATE,
    CASE 
        WHEN 
            Open_Click_Recency_Frequency_Rating = 'MediumGood' or 
            Open_Click_Recency_Frequency_Rating = 'GoodMedium' or 
            Open_Click_Recency_Frequency_Rating = 'GoodGood'
        THEN 'Good'
        WHEN 
            Open_Click_Recency_Frequency_Rating = 'MediumMedium' or
            Open_Click_Recency_Frequency_Rating = 'GoodBad' 
        THEN 'Medium'
        WHEN 
            Open_Click_Recency_Frequency_Rating = 'BadBad' or 
            Open_Click_Recency_Frequency_Rating = 'BadMedium' or 
            Open_Click_Recency_Frequency_Rating = 'BadGood' or 
            Open_Click_Recency_Frequency_Rating = 'MediumBad'
        THEN 'Bad' 
    ELSE NULL END as Susbscriber_Quality     
from concat

)

select * from rf_analysis

