with SEND_SUMMARY as (
    select * from {{ref('int_send_summary')}}
),

OPEN_SUMMARY as (
    select * from {{ref('int_open_summary')}}
),

OPEN_SEND_SUMMARY as 
(
SELECT SEND_SUMMARY.EMAIL,
SEND_SUMMARY.FIRST_SEND,
SEND_SUMMARY.MOST_RECENT_SEND,
CASE WHEN MOST_RECENT_OPEN IS NULL THEN NULL ELSE MOST_RECENT_OPEN END MOST_RECENT_OPEN,
CASE WHEN UNIQUE_OPENS IS NULL THEN 0 ELSE UNIQUE_OPENS END UNIQUE_OPENS,
CASE WHEN CLICKS IS NULL THEN 0 ELSE CLICKS END CLICKS,
SEND_SUMMARY.TOTAL_SENDS,
OPEN_SUMMARY.UNIQUE_OPENS / SEND_SUMMARY.TOTAL_SENDS as "UNIQUE_OPEN_RATE",
OPEN_SUMMARY.CLICKS / SEND_SUMMARY.TOTAL_SENDS as "CLICK_RATE"
FROM SEND_SUMMARY 
LEFT JOIN OPEN_SUMMARY using (EMAIL) 
ORDER BY UNIQUE_OPENS DESC
)

select * from OPEN_SEND_SUMMARY
