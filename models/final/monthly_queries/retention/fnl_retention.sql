-- this query takes a really long time to run, filter for one email at a time or run in snowflake

with retention as (
    select * from {{ref('int1_retention')}}
),

aggregated_opens_clicks as (
    select * from {{ref('int4_aggregated_opens_clicks')}}
),

aggregated_opens_clicks_retention as ( 
    select
        retention.Email,
        retention.Growth_Channel,
        retention.Status,
        retention.Country,
        retention.City,
        retention.First_Email,
        retention.Last_Email,
        retention.Days_Retained,
        retention.Total_Campaigns,
        aggregated_opens_clicks.Total_Opens,
        aggregated_opens_clicks.Lifetime_Unique_Opens,
        aggregated_opens_clicks.Newsletters_Opened_Same_Day,
        aggregated_opens_clicks.Total_Clicks,
        aggregated_opens_clicks.Lifetime_Unique_Clicks,
        CASE WHEN aggregated_opens_clicks.Total_Clicks > 0 THEN (aggregated_opens_clicks.Total_Clicks / aggregated_opens_clicks.Total_Opens) ELSE 0 END Total_CTOR,
        CASE WHEN aggregated_opens_clicks.Lifetime_Unique_Clicks > 0 THEN (aggregated_opens_clicks.Lifetime_Unique_Clicks / aggregated_opens_clicks.Lifetime_Unique_Opens) ELSE 0 END Unique_CTOR
    from retention 
    LEFT JOIN aggregated_opens_clicks using (Email)
)

select * from aggregated_opens_clicks_retention 
where email = 'laurelb@thegistsports.com'