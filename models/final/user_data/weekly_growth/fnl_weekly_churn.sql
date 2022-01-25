with

SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

lost_subscribers as (
SELECT 
    date_status_changed Date,
    Growth_Channel,
    status,
    coalesce(Country, 'US') Country,
    coalesce(Cities, 'None') Cities,
    source_brand,
    campaign_name,
    -1*(count(EMAIL)) as Churn
FROM SUBSCRIBERS 
WHERE date_status_changed >'2022-01-16' 
AND date_status_changed < '2022-01-24'
AND status <> 'Active'
Group by 1,2,3,4,5,6, 7
ORDER BY 1 DESC
)

select * from lost_subscribers
limit 100000
