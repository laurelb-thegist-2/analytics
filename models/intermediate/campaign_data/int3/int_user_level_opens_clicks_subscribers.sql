with user_level_opens_subscribers as (
    select * from {{ref('int_user_level_opens_subscribers')}}
),

user_level_clicks_subscribers as (
    select * from {{ref('int_user_level_clicks_subscribers')}}
),

user_level_opens_clicks_subscribers as (
    select 
    user_level_opens_subscribers.*,
    user_level_clicks_subscribers.Total_Clicks,
    user_level_clicks_subscribers.Unique_Clicks
    from user_level_opens_subscribers
    LEFT JOIN user_level_clicks_subscribers 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY)
)

select * from user_level_opens_clicks_subscribers