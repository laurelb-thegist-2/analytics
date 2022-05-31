-- pulls from int2_opens in 02_growth -> b_retention

with opens as (
    select * from {{ref('int2_opens')}}
),

opens_by_user as (
    select
        Email,
        MIN(CAST(First_Open as Date)) First_Open,
        MAX(CAST(Most_Recent_Open as Date)) Most_Recent_Open,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Unique_Opens
    from opens
    GROUP BY 1
)

SELECT * FROM opens_by_user