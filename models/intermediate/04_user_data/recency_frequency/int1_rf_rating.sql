with user_data as (
    select * from {{ref('fnl_user_data')}}
),

rfm_analysis as (
select
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    Email,
    Status,
    Country,
    Cities,
    Delivered,
    MOST_RECENT_OPEN,
    CASE WHEN MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) THEN 5
         WHEN MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) THEN 4
         WHEN MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) THEN 3
         WHEN MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) THEN 2
         WHEN MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) THEN 1
    ELSE 1 END as Recency_Rating,
    total_opens,
    UNIQUE_OPEN_RATE,
    CASE WHEN UNIQUE_OPEN_RATE >= 0.9 THEN 5
         WHEN UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6 THEN 4
         WHEN UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3 THEN 3
         WHEN UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0 THEN 2
         WHEN UNIQUE_OPEN_RATE = 0 THEN 1
    ELSE 1 END as Frequency_Rating
from user_data
where Status = 'Active'
)

select
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    Email,
    Status,
    Country,
    Cities,
    Delivered,
    MOST_RECENT_OPEN,
    Recency_Rating,
    total_opens,
    UNIQUE_OPEN_RATE,
    Frequency_Rating,
    concat(Recency_Rating, Frequency_Rating) as Recency_Frequency
from rfm_analysis


