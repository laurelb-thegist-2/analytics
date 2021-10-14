with user_level_opens_subscribers as (
    select * from {{ref('int_user_level_opens_subscribers')}}
),

user_level_clicks_subscribers as (
    select * from {{ref('int_user_level_clicks_subscribers')}}
),

user_level_opens_clicks_subscribers as (
    select 
        user_level_opens_subscribers.*,
        user_level_clicks_subscribers.total_clicks,
        user_level_clicks_subscribers.unique_clicks
    from user_level_opens_subscribers
    LEFT JOIN user_level_clicks_subscribers using (Campaign_ID)
    WHERE CAMPAIGN_DATE > '2021-10-01'
    GROUP BY CAMPAIGN_DATE, NAME, Campaign_ID, COUNTRY
    ORDER BY CAMPAIGN_DATE DESC
)

select * from user_level_opens_clicks_subscribers