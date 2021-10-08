with EMAIL_SENDS as (
    select * from {{ref('stg_email_sends')}}
)

with CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
)

with SEND_SUMMARY as (
    SELECT EMAIL,
        MIN(CAMPAIGN_DATE) FIRST_SEND,
        MAX(CAMPAIGN_DATE) MOST_RECENT_SEND,
        count(distinct a.CAMPAIGN_ID) TOTAL_SENDS
    from EMAIL_SENDS a
LEFT JOIN CAMPAIGN_DETAILS b 
ON a.CAMPAIGN_ID = b.CAMPAIGN_ID
WHERE NAME ilike '%newsletter%' 
GROUP BY 1
)