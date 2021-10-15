with user_level_opens_subscribers as (
    select * from {{ref('int_user_level_opens_subscribers')}}
),

user_level_clicks_subscribers as (
    select * from {{ref('int_user_level_clicks_subscribers')}}
),

user_level_opens_clicks_subscribers as (
    select 
        user_level_opens_subscribers.NAME,
        user_level_opens_subscribers.CAMPAIGN_DATE,
        user_level_opens_subscribers.Campaign_ID,
        user_level_opens_subscribers.COUNTRY,
        user_level_opens_subscribers.TOTAL_OPENS,
        user_level_opens_subscribers.UNIQUE_OPENS,
        user_level_clicks_subscribers.TOTAL_CLICKS,
        user_level_clicks_subscribers.UNIQUE_CLICKS
    from user_level_opens_subscribers
    LEFT JOIN user_level_clicks_subscribers using (Campaign_ID)
)

select * from user_level_opens_clicks_subscribers