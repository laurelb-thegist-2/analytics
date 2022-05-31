with int1_recency_frequency as (
    select * from {{ref('int1_rf_opens_rating')}}
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
    Recency_Rating,
    Unique_Opens,
    UNIQUE_OPEN_RATE,
    Frequency_Rating,
    CASE 
        WHEN 
            Recency_Frequency = 34 or 
            Recency_Frequency = 35 or 
            Recency_Frequency = 44 or 
            Recency_Frequency = 45 or 
            Recency_Frequency = 54 or 
            Recency_Frequency = 55 or
            Recency_Frequency = 43 or 
            Recency_Frequency = 53
        THEN 'Good'
        WHEN 
            Recency_Frequency = 23 or
            Recency_Frequency = 24 or 
            Recency_Frequency = 25 or 
            Recency_Frequency = 33 or 
            Recency_Frequency = 42 or 
            Recency_Frequency = 52 or
            Recency_Frequency = 32 or 
            Recency_Frequency = 33
        THEN 'Medium'
        WHEN 
            Recency_Frequency = 11 or 
            Recency_Frequency = 12 or 
            Recency_Frequency = 13 or 
            Recency_Frequency = 14 or 
            Recency_Frequency = 15 or 
            Recency_Frequency = 21 or 
            Recency_Frequency = 22 or 
            Recency_Frequency = 31 or 
            Recency_Frequency = 41 or 
            Recency_Frequency = 51 
        THEN 'Bad' 
    ELSE NULL END as Open_Recency_Frequency_Rating     
from int1_recency_frequency
)

select * from rf_analysis

