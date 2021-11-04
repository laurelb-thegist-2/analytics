with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

user_data_by_growth_channel as (
SELECT SUBSCRIBERS.Growth_Channel,
    count(SUBSCRIBERS.EMAIL) as Emails,
    sum(total_sends) as SENDS,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    coalesce(sum(unique_opens)/sum(total_sends), 0) as OPEN_RATE,
    coalesce(sum(total_clicks)/sum(total_sends), 0) as CLICK_RATE
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE SUBSCRIBERS.status ilike 'Active' 
and OPEN_SEND_CLICK_SUMMARY.FIRST_SEND >'2021-09-28' 
Group by 1
)

select * from user_data_by_growth_channel
ORDER BY EMAILS DESC
