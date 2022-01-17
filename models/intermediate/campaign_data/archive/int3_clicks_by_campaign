with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

user_level_clicks as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        EMAIL_EVENTS.Campaign_ID,
        EMAIL_EVENTS.email,
        URL,
        email_events.timestamp,
        EMAIL_EVENTS.ACTION
    from email_events 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
WHERE ACTION = 'click'
)

select * from user_level_clicks