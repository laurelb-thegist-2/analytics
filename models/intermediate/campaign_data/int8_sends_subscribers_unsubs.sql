with sends_subscribers as (
    select * 
    from {{ref('int4_sends_subs_by_campaign')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

sends_subscribers_unsubs as (
    select
        sends_subscribers.CAMPAIGN_ID, 
        sends_subscribers.Name,
        sends_subscribers.CAMPAIGN_DATE,
        sends_subscribers.Country,
        sends_subscribers.Total_Sends,
        CAMPAIGN_DETAILS.Total_Unsubscribes
    from sends_subscribers
    LEFT JOIN CAMPAIGN_DETAILS using (CAMPAIGN_ID, Name, CAMPAIGN_DATE)
ORDER BY 3 DESC
)

select * from sends_subscribers_unsubs


