with OPEN_SEND_SUMMARY as (
    select * from {{ref('int_open_send_summary')}}
),

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
)

SELECT SUBSCRIBERS.Growth_Channel,
count(SUBSCRIBERS.Email) as Emails,
sum(total_sends) as SENDS,
sum(unique_opens) as UNIQUE_OPENS,
sum(unique_opens)/sum(total_sends) as OPEN_RATE
FROM OPEN_SEND_SUMMARY 
LEFT JOIN SUBSCRIBERS using (EMAIL) 
Where SUBSCRIBERS.status ilike 'Active' and OPEN_SEND_SUMMARY.FIRST_SEND >'2021-09-20' 
Group by 1
ORDER BY 2 DESC