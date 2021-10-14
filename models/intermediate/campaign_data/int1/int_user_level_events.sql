with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

user_level_events as (
    select
        CAMPAIGN_DATE,
        Campaign_ID,
        NAME,
        EMAIL_EVENTS.email,
        Action
    from email_events 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
)

select * from user_level_events