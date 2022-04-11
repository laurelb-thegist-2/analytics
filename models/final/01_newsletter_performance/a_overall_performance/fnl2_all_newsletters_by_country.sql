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
    CASE WHEN NAME ilike '%pop-up%' or NAME ilike '%olympics%' THEN 'Special Edition' ELSE 'Regular' END Newsletter_Type,
    sum(sends_subscribers.Total_Sends) Sends,
    sum(sends_subscribers.Total_Bounced) Bounces,
    sum(sends_subscribers.Delivered_Emails) Delivered,
    sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers.Delivered_Emails END) Total_Dojo_Sends,
    sum(sends_subscribers.Delivered_Emails) - sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers.Delivered_Emails END) Total_Regular_Sends,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Gmail_Total_Opens) Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Non_Gmail_Total_Opens) Non_Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Opens) / sum(sends_subscribers.Delivered_Emails) Total_Open_Rate,
    sum(opens_clicks_subscribers.Unique_Opens) / sum(sends_subscribers.Delivered_Emails) Unique_Open_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(sends_subscribers.Delivered_Emails) Total_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(sends_subscribers.Delivered_Emails)  Unique_Click_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_CTOR,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR,
    sum(opens_clicks_subscribers.Total_Partner_Clicks) Total_Partner_Clicks,
    sum(opens_clicks_subscribers.Unique_Partner_Clicks) Unique_Partner_Clicks,
    sum(opens_clicks_subscribers.Total_Partner_Clicks) / sum(sends_subscribers.Delivered_Emails) Total_Partner_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Partner_Clicks) / sum(sends_subscribers.Delivered_Emails)  Unique_Partmer_Click_Rate,
    sum(opens_clicks_subscribers.Total_Partner_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_Partner_CTOR,
    sum(opens_clicks_subscribers.Unique_Partner_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_Partner_CTOR
    from opens_clicks_subscribers
    RIGHT JOIN sends_subscribers using (Campaign_ID, Name, Campaign_Date, Country, City, Growth_Channel, Growth_Bucket, Incentivization)
    GROUP BY 1,2,3
)

select *
from campaign_data_by_country
WHERE Campaign_Date is not null and Campaign_Date > '2022-03-15'
ORDER BY 1,2