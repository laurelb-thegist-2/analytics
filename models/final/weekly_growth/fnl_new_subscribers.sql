with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int5_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

new_subs_by_growth_channel as (
SELECT SUBSCRIBERS.Growth_Channel Growth_Channel_DBT,
    SUBSCRIBERS.source_brand,
    SUBSCRIBERS.campaign_name,
    count(SUBSCRIBERS.EMAIL) as GROWTH
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE OPEN_SEND_CLICK_SUMMARY.FIRST_SEND >'2021-10-31' 
AND OPEN_SEND_CLICK_SUMMARY.FIRST_SEND < '2021-11-08'
Group by 1,2,3
ORDER BY 1 DESC
)

select * from new_subs_by_growth_channel
limit 10000
