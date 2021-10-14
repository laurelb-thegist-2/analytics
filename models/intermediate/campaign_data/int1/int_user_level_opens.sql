with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

user_level_opens as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        EMAIL_EVENTS.Campaign_ID,
        EMAIL_EVENTS.email,
        EMAIL_EVENTS.Action
    from email_events 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
WHERE ACTION = 'open' AND CAMPAIGN_DATE IS NOT NULL
ORDER BY CAMPAIGN_DATE DESC
)

select * from user_level_opens