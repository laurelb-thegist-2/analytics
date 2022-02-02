with campaign_data as (
    select * from {{ref('int1_opens_clicks_sends')}}
),

non_incent as (
    select
        Campaign_Date,
        Country,
        --City,
        CASE WHEN Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%' THEN 'Non-Incentivized' END Incentivization,
        sum(Delivered) Delivered,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Unique_Opens,
        sum(Unique_Opens) / sum(Delivered) Unique_Open_Rate,
        sum(Total_Clicks) Total_Clicks,
        sum(Unique_Clicks) Unique_Clicks,
        sum(Total_Clicks) / sum(Total_Opens) Total_CTOR
    from campaign_data
    where Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%'
    GROUP BY 1,2,3
    ORDER BY 1,2
)

select * from non_incent 