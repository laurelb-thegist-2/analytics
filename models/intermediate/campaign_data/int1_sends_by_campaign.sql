with EMAIL_SENDS as (
    select * from {{ref('stg_email_sends')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

sends as (
    select
        CAMPAIGN_DATE,
        Campaign_ID,
        NAME,
        EMAIL_SENDS.email
    from email_sends 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
)

select * from sends