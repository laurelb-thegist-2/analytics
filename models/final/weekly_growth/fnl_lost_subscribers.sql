with

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

lost_subscribers as (
SELECT Growth_Channel,
    status,
    source_brand,
    campaign_name,
    count(EMAIL) as Churn
FROM SUBSCRIBERS 
WHERE date_status_changed >'2021-10-31' 
AND date_status_changed < '2021-11-08'
AND status <> 'Active'
Group by 1,2,3,4
ORDER BY 1 DESC
)

select * from lost_subscribers
