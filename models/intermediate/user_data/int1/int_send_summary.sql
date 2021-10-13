with EMAIL_SENDS as (
    select * from {{ref('stg_email_sends')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

SEND_SUMMARY as (
SELECT EMAIL,
MIN(CAMPAIGN_DATE) FIRST_SEND,
MAX(CAMPAIGN_DATE) MOST_RECENT_SEND,
count(distinct EMAIL_SENDS.CAMPAIGN_ID) TOTAL_SENDS
from EMAIL_SENDS
LEFT JOIN CAMPAIGN_DETAILS using (CAMPAIGN_ID)
WHERE NAME ilike '%newsletter%' 
GROUP BY 1
)

select * from SEND_SUMMARY