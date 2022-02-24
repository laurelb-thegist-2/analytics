with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int4_final_subscribers')}}
),

deleted_subs as (
SELECT 
    SUBSCRIBERS.Growth_Bucket,
    SUBSCRIBERS.Incentivization,
    count(Email) Deleted_Volume,
    sum(unique_opens)/sum(delivered) Deleted_UNIQUE_OPEN_RATE,
    sum(total_clicks)/sum(delivered) Deleted_CLICK_RATE,
    sum(total_clicks)/sum(total_opens) Deleted_TOTAL_CTOR
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE FIRST_SEND > '2021-12-31' and Status = 'Deleted'
Group by 1,2
)

select *
from deleted_subs
ORDER BY 1

