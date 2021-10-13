with EMAIL_EVENTS as (
    select * from {{ref('stg_email_events')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

CLICK_SUMMARY as (
    SELECT EMAIL,
        MIN(CAMPAIGN_DATE) FIRST_CLICK,
        MAX(CAMPAIGN_DATE) MOST_RECENT_CLICK,
        count(distinct EMAIL_EVENTS.Campaign_ID) CAMPAIGNS_CLICKED,
        count(EMAIL_EVENTS.URL) CLICKS
    from EMAIL_EVENTS 
    LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID) 
    WHERE NAME ilike '%newsletter%' and EMAIL_EVENTS.URL is not null
    GROUP BY 1
)

select * from CLICK_SUMMARY

