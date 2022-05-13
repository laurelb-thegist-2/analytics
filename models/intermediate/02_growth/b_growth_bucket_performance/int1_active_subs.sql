-- pulls from user queries in the intermediate -> 04_user_data folders

with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int4_final_subscribers')}}
),

active_subs as (
SELECT 
    SUBSCRIBERS.Growth_Bucket,
    SUBSCRIBERS.Growth_Int_Bucket,
    --SUBSCRIBERS.Growth_Channel,
    SUBSCRIBERS.Incentivization,
    count(Email) Active_Volume,
    CASE WHEN sum(delivered) > 0 THEN sum(unique_opens)/sum(delivered) ELSE 0 END Active_UNIQUE_OPEN_RATE,
    CASE WHEN sum(delivered) > 0 THEN sum(total_clicks)/sum(delivered) ELSE 0 END Active_CLICK_RATE,
    CASE WHEN sum(total_opens) > 0 THEN sum(total_clicks)/sum(total_opens) ELSE 0 END Active_TOTAL_CTOR,
    CASE WHEN sum(delivered) > 0 THEN sum(total_partner_clicks)/sum(delivered) ELSE 0 END Active_Partner_Click_Rate,
    CASE WHEN sum(total_opens) > 0 THEN sum(total_partner_clicks)/sum(total_opens) ELSE 0 END Active_Partner_Total_CTOR
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE FIRST_SEND < '2022-03-31' and FIRST_SEND > '2021-12-31' and Status = 'Active'
Group by 1,2,3--,4
)

select *
from active_subs
ORDER BY 1

