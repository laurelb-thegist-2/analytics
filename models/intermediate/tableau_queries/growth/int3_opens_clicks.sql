-- pulls from int3_clicks_by_campaign (found under folder campaign_data), which combines campaign_clicks and campaign_details, with boardman clicks filtered out

with opens as (
    select * from {{ref('int2_opens')}}
),

clicks as (
    select * from {{ref('int3_clicks_by_campaign')}}
),

opens_clicks as (
    select
        opens.Email,
        opens.Campaign_ID,
        opens.Campaign_Date,
        opens.Name,
        opens.First_Open,
        opens.Total_Opens,
        opens.Unique_Opens,
        count(clicks.email) Total_Clicks,
        count(distinct clicks.email) Newsletters_Clicked,
        CASE WHEN opens.Unique_Opens > 0 AND DATEDIFF(day, opens.Campaign_Date, opens.First_Open) = 1 THEN 1 ELSE 0 END Opened_Same_Day
    from opens
    LEFT JOIN clicks using (Campaign_ID, Campaign_Date, Email)
    GROUP BY 1,2,3,4,5,6,7
)

select * from opens_clicks