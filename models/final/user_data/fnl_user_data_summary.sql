with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

user_data_summary as (
SELECT SUBSCRIBERS.EMAIL as Email, 
    SUBSCRIBERS.Growth_Channel,
    SUBSCRIBERS.status,
    SUBSCRIBERS.Country,
    SUBSCRIBERS.Cities,
    OPEN_SEND_CLICK_SUMMARY.FIRST_SEND,
    SUBSCRIBERS.date_status_changed,
    sum(total_sends) as SENDS,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    sum(unique_opens)/sum(total_sends) as OPEN_RATE,
    sum(total_clicks)/sum(total_sends) as CLICK_RATE
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL) 
where email ilike '%burnertest%'
Group by 1,2,3,4,5,6,7
)

select * from user_data_summary
