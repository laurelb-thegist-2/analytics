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
    CASE WHEN MOST_RECENT_OPEN >= dateadd(week, -2, GETDATE()) THEN 5
         WHEN MOST_RECENT_OPEN < dateadd(week, -2, GETDATE()) and MOST_RECENT_OPEN >= dateadd(month, -1, GETDATE()) THEN 4
         WHEN MOST_RECENT_OPEN < dateadd(month, -1, GETDATE()) and MOST_RECENT_OPEN >= dateadd(month, -3, GETDATE()) THEN 3
         WHEN MOST_RECENT_OPEN < dateadd(month, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(month, -6, GETDATE()) THEN 2
         WHEN MOST_RECENT_OPEN < dateadd(month, -6, GETDATE()) THEN 1
    ELSE 1 END as Recency_Rating,
    total_opens,
    unique_opens,
    UNIQUE_OPEN_RATE,
    CASE WHEN UNIQUE_OPEN_RATE >= 0.75 THEN 5
         WHEN UNIQUE_OPEN_RATE < 0.75 AND UNIQUE_OPEN_RATE >= 0.5 THEN 4
         WHEN UNIQUE_OPEN_RATE < 0.5 and UNIQUE_OPEN_RATE >= 0.35 THEN 3
         WHEN UNIQUE_OPEN_RATE < 0.35 and UNIQUE_OPEN_RATE >= 0.25 THEN 2
         WHEN UNIQUE_OPEN_RATE < 0.25 THEN 1
    ELSE 1 END as Frequency_Rating,
    Total_clicks,
    CLICK_RATE,
    CASE WHEN CLICK_RATE > 20 and DELIVERED > 9 THEN 1
        WHEN CLICK_RATE >= 0.50 THEN 5
        WHEN CLICK_RATE < 0.50 AND CLICK_RATE >= 0.30 THEN 4
        WHEN CLICK_RATE < 0.30 and CLICK_RATE >= 0.10 THEN 3
        WHEN CLICK_RATE < 0.10 and CLICK_RATE > 0 THEN 2
        WHEN CLICK_RATE = 0 THEN 1
    ELSE 1 END as Monetary_Rating
from user_data
where Status = 'Active'
)

select * from rfm_analysis


