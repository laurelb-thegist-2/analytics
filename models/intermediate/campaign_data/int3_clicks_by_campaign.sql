with campaign_clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

user_level_clicks as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        campaign_clicks.Campaign_ID,
        campaign_clicks.email,
        URL,
        campaign_clicks.timestamp
    from campaign_clicks 
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
)

select * from user_level_clicks
limit 10000