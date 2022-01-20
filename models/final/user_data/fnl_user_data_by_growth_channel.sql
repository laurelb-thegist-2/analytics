with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

user_data_by_growth_channel as (
SELECT SUBSCRIBERS.Growth_Channel,
    FIRST_SEND,
    SUBSCRIBERS.Email,
    SUBSCRIBERS.Status,
    SUBSCRIBERS.campaign_name,
    SUBSCRIBERS.source_brand,
    sum(total_sends) as SENDS,
    sum(unique_opens) as UNIQUE_OPENS,
    sum(total_clicks) as TOTAL_CLICKS,
    coalesce(sum(unique_opens)/sum(total_sends), 0) as UNIQUE_OPEN_RATE,
    coalesce(sum(total_clicks)/sum(total_sends), 0) as CLICK_RATE
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
Group by 1,2,3,4,5,6
)

select * from user_data_by_growth_channel
WHERE FIRST_SEND < '2022-01-01' and FIRST_SEND > '2020-12-31'
ORDER BY 1,2,3,4,5,6
