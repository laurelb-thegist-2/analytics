with OPEN_SEND_SUMMARY as (
    select * from {{ref('int4_sends_opens_by_user')}}
),

CLICK_SUMMARY as (
    select * from {{ref('int3_clicks_by_user')}}
),

open_send_click_summary as
(
    SELECT 
        OPEN_SEND_SUMMARY.EMAIL,
        OPEN_SEND_SUMMARY.FIRST_SEND,
        OPEN_SEND_SUMMARY.MOST_RECENT_SEND,
        OPEN_SEND_SUMMARY.FIRST_OPEN,
        OPEN_SEND_SUMMARY.MOST_RECENT_OPEN,
        CLICK_SUMMARY.FIRST_CLICK,
        CLICK_SUMMARY.MOST_RECENT_CLICK,
        OPEN_SEND_SUMMARY.TOTAL_SENDS,
        OPEN_SEND_SUMMARY.TOTAL_BOUNCED,
        OPEN_SEND_SUMMARY.Delivered,
        OPEN_SEND_SUMMARY.UNIQUE_OPENS,
        OPEN_SEND_SUMMARY.TOTAL_OPENS,
        CLICK_SUMMARY.CAMPAIGNS_CLICKED,
        CLICK_SUMMARY.TOTAL_CLICKS
    FROM OPEN_SEND_SUMMARY 
    LEFT JOIN CLICK_SUMMARY using (EMAIL) 
)

select * from open_send_click_summary