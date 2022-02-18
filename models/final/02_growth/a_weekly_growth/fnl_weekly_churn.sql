with SUBSCRIBERS as (
    select * from {{ref('int4_subs_growth_channel')}}
),

lost_subscribers as (
SELECT 
    date_status_changed Date,
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    status,
    coalesce(Country, 'US') Country,
    coalesce(Cities, 'None') Cities,
    source_brand,
    campaign_name,
    -1*(count(EMAIL)) as Churn
FROM SUBSCRIBERS 
WHERE date_status_changed > '2022-02-06' 
AND date_status_changed < '2022-02-14'
AND status <> 'Active'
Group by 1,2,3,4,5,6,7,8,9
ORDER BY 1 DESC
)

select * from lost_subscribers
limit 100000
