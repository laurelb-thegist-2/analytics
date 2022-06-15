with user_data as (
    select * from {{ref('fnl_user_data')}}
),

rfm_analysis as (
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
    MOST_RECENT_CLICK,
    CASE WHEN MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) THEN 5
         WHEN MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE()) THEN 4
         WHEN MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) THEN 3
         WHEN MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) THEN 2
         WHEN MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL THEN 1
    ELSE 1 END as Recency_Rating,
    Total_Clicks,
    Click_Rate,
    CASE WHEN Click_Rate <= 10 AND Click_Rate >= 0.1 THEN 5
         WHEN Click_Rate < 0.1 AND Click_Rate >= 0.05 THEN 4
         WHEN Click_Rate < 0.05 and Click_Rate >= 0.01 THEN 3
         WHEN Click_Rate < 0.01 and Click_Rate > 0 THEN 2
         WHEN Click_Rate = 0 or Click_Rate > 10  THEN 1
    ELSE 1 END as Frequency_Rating
from user_data
)

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
    MOST_RECENT_CLICK,
    Recency_Rating,
    Total_Clicks,
    Click_Rate,
    Frequency_Rating,
    concat(Recency_Rating, Frequency_Rating) as Recency_Frequency
from rfm_analysis



