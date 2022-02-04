with incent as (
    select * from {{ref('int2_incent')}}
),

non_incent as (
    select * from {{ref('int3_non_incent')}}
),

incentivization_by_date as (
    select
        Campaign_Date,
        Incentivization,
        sum(Delivered) Delivered,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Unique_Opens,
        sum(Unique_Opens) / sum(Delivered) Unique_Open_Rate,
        sum(Total_Clicks) Total_Clicks,
        sum(Unique_Clicks) Unique_Clicks,
        sum(Total_Clicks) / sum(Total_Opens) Total_CTOR
    from incent
    GROUP BY 1, 2
    UNION
    select 
    Campaign_Date,
        Incentivization,
        sum(Delivered) Delivered,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Unique_Opens,
        sum(Unique_Opens) / sum(Delivered) Unique_Open_Rate,
        sum(Total_Clicks) Total_Clicks,
        sum(Unique_Clicks) Unique_Clicks,
        sum(Total_Clicks) / sum(Total_Opens) Total_CTOR
    from non_incent
    GROUP BY 1, 2
)

select * from incentivization_by_date
WHERE campaign_date = '2022-01-05'