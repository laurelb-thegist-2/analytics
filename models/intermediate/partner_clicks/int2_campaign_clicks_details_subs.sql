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
    from subscribers
    LEFT JOIN campaign_clicks_details using (email)
)

SELECT * FROM campaign_clicks_details_subs
where Campaign_ID = 'f7464ac18168ef72f30fbc6af76e164c'
limit 100000
