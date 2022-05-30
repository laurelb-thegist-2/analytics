with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int2_final_subscribers')}}
),

user_data_summary as (
    SELECT 
        SUBSCRIBERS.EMAIL as Email,
        SUBSCRIBERS.LEADID, 
        SUBSCRIBERS.Growth_Bucket,
        SUBSCRIBERS.status,
        SUBSCRIBERS.Partner_Engagement_Surveys,
        SUBSCRIBERS.referral_code,
        SUBSCRIBERS.referral_count,
        coalesce(subscribers.Country, 'US') Country,
        coalesce(SUBSCRIBERS.Cities, 'None') Cities,
        OPEN_SEND_CLICK_SUMMARY.FIRST_SEND,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_SEND,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_SEND - OPEN_SEND_CLICK_SUMMARY.FIRST_SEND Lifetime_in_days,
        OPEN_SEND_CLICK_SUMMARY.FIRST_OPEN,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_OPEN,
        OPEN_SEND_CLICK_SUMMARY.FIRST_CLICK,
        OPEN_SEND_CLICK_SUMMARY.MOST_RECENT_CLICK,
        SUBSCRIBERS.date_status_changed,
        sum(total_sends) as SENDS,
        sum(total_bounced) as BOUNCED,
        sum(delivered) as DEVLIERED,
        sum(unique_opens) as UNIQUE_OPENS,
        sum(total_opens) as TOTAL_OPENS,
        sum(total_clicks) as TOTAL_CLICKS,
        sum(total_partner_clicks) as PARTNER_TOTAL_CLICKS,
        CASE WHEN sum(delivered) > 0 THEN sum(unique_opens)/sum(delivered) ELSE 0 END as UNIQUE_OPEN_RATE,
        CASE WHEN sum(total_opens) > 0 THEN sum(total_clicks)/sum(total_opens) ELSE 0 END TOTAL_CTOR,
        CASE WHEN sum(total_opens) > 0 THEN sum(total_clicks)/sum(delivered) ELSE 0 END CLICK_RATE,
        CASE WHEN sum(total_opens) > 0 THEN sum(total_partner_clicks)/sum(total_opens) ELSE 0 END PARTNER_TOTAL_CTOR,
        CASE WHEN sum(total_opens) > 0 THEN sum(total_partner_clicks)/sum(delivered) ELSE 0 END PARTNER_CLICK_RATE
    FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL) 
Group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)

select *
from user_data_summary 
where Growth_Bucket = 'Organic/Unknown' and First_Send > '2021-12-31'
limit 100000
