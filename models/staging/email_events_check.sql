with email_events as (
    select * from {{ref('stg_email_events')}}
),

campaign_details as (
    select * from {{ref('stg_campaign_details')}}
),

email_events_campaign_details as (
    select 
        campaign_Details.CAMPAIGN_ID,
        campaign_details.NAME,
        campaign_details.Campaign_date,
        campaign_details.total_sends,
        campaign_Details.total_opens,
        campaign_Details.unique_opens,
        COUNT(email_events.email) email_events_total_opens,
        COUNT(distinct email_events.email) email_events_unique_opens
    FROM email_events
    LEFT JOIN campaign_details using (CAMPAIGN_ID)
    WHERE Campaign_date > '2021-01-01' and email_events.Action ilike '%open%'
    GROUP BY 1,2,3,4,5,6
    order by Campaign_date 
)

SELECT * FROM EMAIL_EVENTS_CAMPAIGN_DETAILS
