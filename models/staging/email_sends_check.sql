with email_sends as (
    select * from {{ref('stg_email_sends')}}
),

campaign_details as (
    select * from {{ref('stg_campaign_details')}}
),

email_sends_campaign_details as (
    select 
        email_sends.CAMPAIGN_ID,
        campaign_details.NAME,
        campaign_details.Campaign_date,
        campaign_details.total_sends,
        COUNT(email_sends.email)
    FROM email_sends
    LEFT JOIN campaign_details using (CAMPAIGN_ID)
    WHERE Campaign_date > '2021-10-01'
    GROUP BY 1,2,3,4
)

SELECT * FROM EMAIL_SENDS_CAMPAIGN_DETAILS
