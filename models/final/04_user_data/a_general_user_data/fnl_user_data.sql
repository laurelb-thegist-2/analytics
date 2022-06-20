with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

user_data_by_growth_channel as (
SELECT 
    SUBSCRIBERS.Growth_Summary,
    SUBSCRIBERS.Growth_Bucket,
    SUBSCRIBERS.Growth_Int_Bucket,
    SUBSCRIBERS.Growth_Channel,
    SUBSCRIBERS.CITIES,
    SUBSCRIBERS.COUNTRY,
    SUBSCRIBERS.Email,
    SUBSCRIBERS.Status,
    FIRST_SEND,
    MOST_RECENT_SEND,
    FIRST_OPEN,
    MOST_RECENT_OPEN,
    MOST_RECENT_CLICK,
    sum(delivered) AS DELIVERED,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_opens) as TOTAL_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    case when sum(delivered) > 0 then sum(unique_opens)/sum(delivered) else 0 end as UNIQUE_OPEN_RATE,
    case when sum(delivered) > 0 then sum(total_clicks)/sum(delivered) else 0 end as CLICK_RATE,
    case when sum(total_opens) > 0 then sum(total_clicks)/sum(total_opens) else 0 end as TOTAL_CTOR
FROM SUBSCRIBERS
LEFT JOIN OPEN_SEND_CLICK_SUMMARY using (EMAIL)
Group by 1,2,3,4,5,6,7,8,9,10,11,12,13
)

select * from user_data_by_growth_channel
--WHERE FIRST_SEND > dateadd(week, -2, GETDATE()) and CITIES = 'DAL' and Growth_Channel ilike '%organic/unknown%' and status = 'Active'
--WHERE FIRST_SEND > dateadd(week, -2, GETDATE()) and CITIES = 'SEA' and Growth_Channel ilike '%organic/unknown%' and status = 'Active'
ORDER BY 1,2,3,4,5,6
limit 1000000


