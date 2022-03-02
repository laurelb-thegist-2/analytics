with user_data_summary as (
    select * from {{ref('fnl_user_data_summary')}}
)

SELECT 
    Email, 
    status,
    Partner_Engagement_Surveys,
    coalesce(Country, 'US') Country,
    cast(FIRST_SEND as DATE) FIRST_SEND,
    coalesce(sum(unique_opens)/sum(DEVLIERED), 0) as UNIQUE_OPEN_RATE
FROM user_data_summary
WHERE FIRST_SEND < '2022-01-01' and UNIQUE_OPEN_RATE > 0.25 and status = 'Active'
and Partner_Engagement_Surveys not ilike '%Under Armour%' and Partner_Engagement_Surveys not ilike '%Yes%'
GROUP BY 1,2,3,4,5

limit 10000


