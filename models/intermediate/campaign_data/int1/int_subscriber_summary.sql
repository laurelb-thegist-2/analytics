with SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

SUBSCRIBER_SUMMARY as (
SELECT EMAIL,
    Growth_Channel,
    Cities,
    Upper(Country) as Country,
    Status
FROM SUBSCRIBERS where list_id = '54eb7610971ecdad5354d8d07b2b6397'
)

select * from SUBSCRIBER_SUMMARY