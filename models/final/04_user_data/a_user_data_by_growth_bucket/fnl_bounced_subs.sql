with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int4_final_subscribers')}}
),

Bounced_subs as (
SELECT 
    SUBSCRIBERS.Growth_Bucket,
    SUBSCRIBERS.Incentivization,
    count(Email) Bounced_Volume,
    sum(unique_opens)/sum(delivered) Bounced_UNIQUE_OPEN_RATE,
    sum(total_clicks)/sum(delivered) Bounced_CLICK_RATE,
    sum(total_clicks)/sum(total_opens) Bounced_TOTAL_CTOR
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE FIRST_SEND > '2021-12-31' and Status = 'Bounced'
Group by 1,2
)

select *
from Bounced_subs
ORDER BY 1

