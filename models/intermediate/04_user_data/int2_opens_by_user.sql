with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

OPENS as (
    select * from {{ref('stg_campaign_opens')}}
),

OPEN_SUMMARY as (
    SELECT 
        EMAIL,
        MIN(CAMPAIGN_DATE) FIRST_OPEN,
        MAX(CAMPAIGN_DATE) MOST_RECENT_OPEN,
        count(opens.email) Total_Opens,
        count(distinct EMAIL_EVENTS.Campaign_ID) UNIQUE_OPENS
    from EMAIL_EVENTS 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
LEFT JOIN OPENS using (Campaign_ID)
GROUP BY 1
)

select * from OPEN_SUMMARY