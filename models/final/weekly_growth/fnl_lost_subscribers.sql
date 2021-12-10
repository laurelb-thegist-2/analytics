with

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

lost_subscribers as (
SELECT 
    Growth_Channel,
    status,
    coalesce(Country, 'US') Country,
    coalesce(Cities, 'None') Cities,
    source_brand,
    campaign_name,
    count(EMAIL) as Churn
FROM SUBSCRIBERS 
WHERE date_status_changed >'2021-10-31' 
AND date_status_changed < '2021-11-08'
AND status <> 'Active'
Group by 1,2,3,4,5,6
ORDER BY 1 DESC
)

select * from lost_subscribers
limit 100000
