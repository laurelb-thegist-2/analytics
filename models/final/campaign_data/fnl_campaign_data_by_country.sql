with sends_subscribers as (
    select * 
    from {{ref('int4_sends_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select * 
    from {{ref('int7_opens_clicks_subs_by_campaign')}}
),

campaign_data_by_country as (
    select 
    opens_clicks_subscribers.Campaign_Date,
    opens_clicks_subscribers.Country,
    sum(sends_subscribers.Total_Sends) Total_Sends,
    sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers.Total_Sends END) Total_Dojo_Sends,
    sum(sends_subscribers.Total_Sends) - sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers.Total_Sends END) Total_Regular_Sends,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Opens) / sum(sends_subscribers.Total_Sends) Total_Open_Rate,
    sum(opens_clicks_subscribers.Unique_Opens) / sum(sends_subscribers.Total_Sends) Unique_Open_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(sends_subscribers.Total_Sends) Total_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(sends_subscribers.Total_Sends)  Unique_Click_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_CTOR,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR
    from opens_clicks_subscribers
    RIGHT JOIN sends_subscribers using (Campaign_ID, Name, Campaign_Date, Country)
    -- Where Campaign_Date = '2021-10-18'
    GROUP BY 1,2
    ORDER BY 1,2 DESC
)

select *
from campaign_data_by_country
