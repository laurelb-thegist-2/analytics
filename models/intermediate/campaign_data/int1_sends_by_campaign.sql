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
        Total_Unsubscribes,
        EMAIL_SENDS.email
    from email_sends 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
WHERE CAMPAIGN_DATE = '2021-10-18'
)

select * from sends
limit 1000000