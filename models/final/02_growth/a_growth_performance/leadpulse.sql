-- pulls from user queries in the intermediate -> 04_user_data folders

with OPEN_SEND_CLICK_SUMMARY as (
    select * from {{ref('int4_sends_opens_clicks_by_user')}}
),

SUBSCRIBERS as (
    select * from {{ref('int4_final_subscribers')}}
),

Total_subs as (
SELECT 
    coalesce(SUBSCRIBERS.Growth_Bucket, 'Organic/Unknown') as Growth_Bucket,
    coalesce(SUBSCRIBERS.Growth_Int_Bucket, 'N/A') as Growth_Int_Bucket,
    --SUBSCRIBERS.Growth_Channel,
    coalesce(SUBSCRIBERS.Incentivization, 'Unincentivized') as Incentivization,
    count(Email) Total_Volume,
    sum(CASE WHEN status = 'Active' THEN 1 ELSE 0 END) Active_Volume,
    sum(CASE WHEN FIRST_SEND < '2022-02-01' and FIRST_SEND > '2021-12-31' THEN unique_opens END)/sum(CASE WHEN FIRST_SEND < '2022-02-01' and FIRST_SEND > '2021-12-31' THEN delivered END) Jan_UOR,
    sum(CASE WHEN FIRST_SEND < '2022-03-01' and FIRST_SEND > '2022-01-31' THEN unique_opens END)/sum(CASE WHEN FIRST_SEND < '2022-03-01' and FIRST_SEND > '2022-01-31' THEN delivered END) Feb_UOR,
    sum(CASE WHEN FIRST_SEND < '2022-04-01' and FIRST_SEND > '2022-02-28' THEN unique_opens END)/sum(CASE WHEN FIRST_SEND < '2022-04-01' and FIRST_SEND > '2022-02-28' THEN delivered END) Mar_UOR,
    sum(CASE WHEN FIRST_SEND < '2022-05-01' and FIRST_SEND > '2022-03-31' THEN unique_opens END)/sum(CASE WHEN FIRST_SEND < '2022-05-01' and FIRST_SEND > '2022-03-31' THEN delivered END) Apr_UOR,
    sum(CASE WHEN FIRST_SEND < '2022-06-01' and FIRST_SEND > '2022-04-30' THEN unique_opens END)/sum(CASE WHEN FIRST_SEND < '2022-06-01' and FIRST_SEND > '2022-04-30' THEN delivered END) May_UOR
FROM OPEN_SEND_CLICK_SUMMARY
LEFT JOIN SUBSCRIBERS using (EMAIL)
WHERE Growth_Int_Bucket ilike '%leadpulse%'
Group by 1,2,3
)

select *
from Total_subs 
ORDER BY 1