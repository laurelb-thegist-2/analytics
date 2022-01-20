with opens_clicks as (
    select * from {{ref('int3_opens_clicks')}}
),

aggregated_opens_clicks as (
    select 
        Email,
        sum(Total_Opens) Total_Opens,
        sum(Unique_Opens) Lifetime_Unique_Opens,
        sum(Opened_Same_Day) Newsletters_Opened_Same_Day,
        sum(Total_Clicks) Total_Clicks,
        sum(Newsletters_Clicked) Lifetime_Unique_Clicks
    from opens_clicks
    GROUP BY 1
)

select * from aggregated_opens_clicks