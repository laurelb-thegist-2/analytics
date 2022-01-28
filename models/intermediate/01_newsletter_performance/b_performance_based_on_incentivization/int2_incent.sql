with campaign_data as (
    select * from {{ref('int1_opens_clicks_sends')}}
    where Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%'
),

incent as (
    select
        Campaign_Date,
        Country,
        City,
        CASE WHEN Growth_Channel IS NOT NULL or Growth_Channel is NULL THEN 'Incentivized' END Incentivization,
        sum(Delivered) Delivered,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Unique_Opens,
        sum(Unique_Opens) / sum(Delivered) Unique_Open_Rate,
        sum(Total_Clicks) Total_Clicks,
        sum(Unique_Clicks) Unique_Clicks,
        sum(Total_Clicks) / sum(Total_Opens) Total_CTOR
    from campaign_data
    GROUP BY 1,2,3,4
)

select * from incent 