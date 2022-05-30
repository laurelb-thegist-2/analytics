with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

user_data_summary as (
    SELECT 
        SUBSCRIBERS.Email,
        SUBSCRIBERS.Growth_Channel,
        SUBSCRIBERS.Growth_Int_Bucket,
        OPEN_SEND_CLICK_SUMMARY.FIRST_SEND,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_SEND,
        SUBSCRIBERS.date_status_changed, 
        SUBSCRIBERS.status,
        coalesce(sum(total_sends), 0) as SENDS,
        coalesce(sum(unique_opens), 0) as UNIQUE_OPENS,
        coalesce(sum(total_clicks), 0) as TOTAL_CLICKS,
        coalesce(sum(unique_opens)/sum(total_sends), 0) as UNIQUE_OPEN_RATE,
        coalesce(sum(total_clicks)/sum(total_sends), 0) as CLICK_RATE
    FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL) 
Group by 1,2,3,4,5,6,7
)

select * from user_data_summary
where Growth_Int_Bucket IS NOT NULL AND FIRST_SEND > '2022-01-31' AND FIRST_SEND < '2022-03-01'
limit 100000
