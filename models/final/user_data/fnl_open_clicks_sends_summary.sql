with OPEN_SEND_SUMMARY as (
    select * from {{ref('int_open_send_summary')}}
),

CLICK_SUMMARY as (
    select * from {{ref('int_click_summary')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

OPENS_CLICKS_SENDS_SUMMARY as
(
    SELECT OPEN_SEND_SUMMARY.EMAIL,
        OPEN_SEND_SUMMARY.FIRST_SEND,
        OPEN_SEND_SUMMARY.MOST_RECENT_SEND,
        OPEN_SEND_SUMMARY.MOST_RECENT_OPEN,
        CLICK_SUMMARY.MOST_RECENT_CLICK,
        OPEN_SEND_SUMMARY.TOTAL_SENDS,
        OPEN_SEND_SUMMARY.UNIQUE_OPENS,
        CLICK_SUMMARY.CAMPAIGNS_CLICKED,
        CLICK_SUMMARY.CLICKS as "TOTAL_CLICKS",
        OPEN_SEND_SUMMARY.UNIQUE_OPEN_RATE,
        CLICK_SUMMARY.CAMPAIGNS_CLICKED / OPEN_SEND_SUMMARY.TOTAL_SENDS as "UNIQUE_CLICK_RATE",
        CLICK_SUMMARY.CLICKS / OPEN_SEND_SUMMARY.TOTAL_SENDS as "TOTAL_CLICK_RATE",
        CLICK_SUMMARY.CAMPAIGNS_CLICKED / UNIQUE_OPENS as "CTOR"
    FROM OPEN_SEND_SUMMARY 
    LEFT JOIN CLICK_SUMMARY using (EMAIL) 
)

SELECT SUBSCRIBERS.Growth_Channel,
    count(SUBSCRIBERS.EMAIL) as Emails,
    sum(total_sends) as SENDS,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    sum(unique_opens)/sum(total_sends) as OPEN_RATE,
    sum(total_clicks)/sum(total_sends) as CLICK_RATE
FROM OPENS_CLICKS_SENDS_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
Where SUBSCRIBERS.list_id = '54eb7610971ecdad5354d8d07b2b6397' 
and SUBSCRIBERS.status ilike 'Active' 
and OPENS_CLICKS_SENDS_SUMMARY.FIRST_SEND >'2021-09-28' 
and growth_channel ilike '%leadpulse%'
Group by 1
ORDER BY 2 DESC

