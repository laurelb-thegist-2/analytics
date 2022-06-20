with user_data_summary as (
    select * from {{ref('fnl_user_data_summary')}}
)

SELECT 
    Email, 
    status,
    Partner_Engagement_Surveys,
    coalesce(Country, 'US') Country,
    cast(FIRST_SEND as DATE) FIRST_SEND,
    coalesce(sum(unique_opens)/sum(DEVLIERED), 0) as UNIQUE_OPEN_RATE,
    coalesce(sum(total_clicks)/sum(DEVLIERED), 0) as CLICK_RATE
FROM user_data_summary
WHERE FIRST_SEND < dateadd(month, -1, GETDATE()) and status = 'Active' AND UNIQUE_OPEN_RATE > 0.30 and CLICK_RATE > 0 and Country = 'CA'
and Partner_Engagement_Surveys IS null 
GROUP BY 1,2,3,4,5
limit 2000




