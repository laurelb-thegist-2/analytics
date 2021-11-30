with campaign_clicks_details as (
    select * from {{ref('int1_campaign_clicks_details')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

campaign_clicks_details_subs as (
    select 
        campaign_clicks_details.*,
        coalesce(subscribers.Country, 'US') Country
    from campaign_clicks_details
    LEFT JOIN subscribers using (email)
)

SELECT * FROM campaign_clicks_details_subs
limit 100000