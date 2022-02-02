with incent as (
    select * from {{ref('int2_incent')}}
),

non_incent as (
    select * from {{ref('int3_non_incent')}}
),

incentivization_by_city as (
    select
        Campaign_Date,
        Country,
        City,
        Incentivization,
        Delivered,
        Total_Opens,
        Unique_Opens,
        Unique_Open_Rate,
        Total_Clicks,
        Unique_Clicks,
        Total_CTOR
    from incent
    FULL OUTER JOIN non_incent using (Campaign_Date, Country, City, Incentivization)
)

select * from incentivization_by_city
WHERE campaign_date > '2021-12-31'