with opens_subscribers as (
    select * from {{ref('int5_opens_subs_by_campaign')}}
),

clicks_subscribers as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select 
    opens_subscribers.*,
    clicks_subscribers.Total_Clicks,
    clicks_subscribers.Unique_Clicks
    from opens_subscribers
    LEFT JOIN clicks_subscribers 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY)
)

select * from opens_clicks_subscribers