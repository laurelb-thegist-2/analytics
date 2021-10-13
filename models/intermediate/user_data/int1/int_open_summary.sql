with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

OPEN_SUMMARY as (
    SELECT EMAIL,
        MAX(CAMPAIGN_DATE) MOST_RECENT_OPEN,
        count(distinct EMAIL_EVENTS.Campaign_ID) UNIQUE_OPENS,
        count(EMAIL_EVENTS.URL) Clicks
    from EMAIL_EVENTS 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
WHERE NAME ilike '%newsletter%'
GROUP BY 1
)

select * from OPEN_SUMMARY