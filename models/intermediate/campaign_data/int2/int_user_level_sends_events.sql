with user_level_sends as (
    select * from {{ref('int_user_level_sends')}}
),

USER_LEVEL_EVENTS as (
    select * from {{ref('int_user_level_events')}}
),

user_level_sends_events as (
    SELECT 
        CAMPAIGN_DATE,
        Campaign_ID,
        NAME,
        count(user_level_sends.email) total_sends,
        count(distinct user_level_sends.email) unique_sends
    from user_level_sends 
