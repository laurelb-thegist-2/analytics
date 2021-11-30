with campaign_clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

campaign_details as (
    select * from {{ref('stg_campaign_details')}}
),

campaign_clicks_details as (
    select 
        campaign_clicks.*,
        campaign_details.Campaign_Date,
        campaign_details.NAME
    from campaign_clicks
    LEFT JOIN campaign_details using (Campaign_ID)
)

SELECT * FROM campaign_clicks_details
limit 100000