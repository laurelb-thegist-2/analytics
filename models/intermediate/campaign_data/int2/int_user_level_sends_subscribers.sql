with user_level_sends as (
    select * from {{ref('int_user_level_sends')}}
),

subscriber_summary as (
    select * from {{ref('int_subscriber_summary')}}
),

user_level_sends_subscribers as (
    select 
        user_level_sends.CAMPAIGN_DATE,
        user_level_sends.NAME,
        user_level_sends.Campaign_ID,
        subscriber_summary.Country,
        count(user_level_sends.email) total_sends
    from user_level_sends
    LEFT JOIN subscriber_summary using (email)
    WHERE CAMPAIGN_DATE > '2021-10-01'
    GROUP BY 1,2,3,4
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM user_level_sends_subscribers