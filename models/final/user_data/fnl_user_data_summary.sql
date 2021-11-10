with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

user_data_summary as (
    SELECT 
        SUBSCRIBERS.EMAIL as Email, 
        SUBSCRIBERS.Growth_Channel,
        SUBSCRIBERS.status,
        SUBSCRIBERS.referral_code,
        coalesce(subscribers.Country, 'US') Country,
        coalesce(SUBSCRIBERS.Cities, 'None') Cities,
        OPEN_SEND_CLICK_SUMMARY.FIRST_SEND,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_SEND,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_SEND - OPEN_SEND_CLICK_SUMMARY.FIRST_SEND Lifetime_in_days,
        OPEN_SEND_CLICK_SUMMARY.FIRST_OPEN,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_OPEN,
        OPEN_SEND_CLICK_SUMMARY.FIRST_CLICK,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_CLICK,
        SUBSCRIBERS.date_status_changed,
        coalesce(sum(total_sends), 0) as SENDS,
        coalesce(sum(unique_opens), 0) as UNIQUE_OPENS,
        coalesce(sum(total_clicks), 0) as TOTAL_CLICKS,
        coalesce(sum(unique_opens)/sum(total_sends), 0) as OPEN_RATE,
        coalesce(sum(total_clicks)/sum(total_sends), 0) as CLICK_RATE
    FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL) 
Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
)

select 
   email,
   Growth_Channel,
   status,
   referral_code,
   cast(FIRST_SEND as DATE) first_send,
   MOST_RECENT_SEND,
   OPEN_RATE
from user_data_summary
Where referral_code is null and status = 'Active' and first_send < '2021-10-27'
ORDER BY first_send DESC 
limit 170000
