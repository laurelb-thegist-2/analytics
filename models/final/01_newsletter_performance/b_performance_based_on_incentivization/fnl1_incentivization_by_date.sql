with campaign_data as (
    select * from {{ref('int1_opens_clicks_sends')}}
),

incent as (
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
    from campaign_data
    GROUP BY 1,2
    ORDER BY 1
)

select * from incent 
where campaign_date > '2021-12-31'
order by 1