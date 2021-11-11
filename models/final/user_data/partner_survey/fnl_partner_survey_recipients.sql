with user_data_summary as (
    select * from {{ref('fnl_user_data_summary')}}
)

SELECT 
        Email, 
        status,
        coalesce(Country, 'US') Country,
        cast(FIRST_SEND as DATE) FIRST_SEND,
        coalesce(sum(unique_opens)/sum(sends), 0) as UNIQUE_OPEN_RATE
FROM user_data_summary
WHERE Country = 'US' and FIRST_SEND < '2021-10-15' and UNIQUE_OPEN_RATE > 0.25 and status = 'Active'
GROUP BY 1,2,3,4

limit 300000


