with user_level_opens as (
    select * from {{ref('int_user_level_opens')}}
),

subscriber_summary as (
    select * from {{ref('int_subscriber_summary')}}
),

user_level_opens_subscribers as (
    select 
        user_level_opens.CAMPAIGN_DATE,
        user_level_opens.Name,
        user_level_opens.CAMPAIGN_ID,
        subscriber_summary.Country,
        count(user_level_opens.email) total_opens,
        count(distinct user_level_opens.email) unique_opens
    from user_level_opens
    LEFT JOIN subscriber_summary using (email)
    GROUP BY 1,2,3,4
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM user_level_opens_subscribers