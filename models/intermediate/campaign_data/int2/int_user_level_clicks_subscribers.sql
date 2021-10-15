with user_level_clicks as (
    select * from {{ref('int_user_level_clicks')}}
),

subscriber_summary as (
    select * from {{ref('int_subscriber_summary')}}
),

user_level_clicks_subscribers as (
    select 
        user_level_clicks.CAMPAIGN_DATE,
        user_level_clicks.NAME,
        user_level_clicks.Campaign_ID,
        subscriber_summary.Country,
        count(user_level_clicks.email) total_clicks,
        count(distinct user_level_clicks.email) unique_clicks
    from user_level_clicks
    LEFT JOIN subscriber_summary using (email)
    GROUP BY 1,2,3,4
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM user_level_clicks_subscribers