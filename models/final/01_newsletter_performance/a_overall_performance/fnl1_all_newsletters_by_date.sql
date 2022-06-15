with sends_subscribers_unsubs as (
    select * 
    from {{ref('int8_sends_subscribers_unsubs')}}
),

opens_clicks_subscribers as (
    select * 
    from {{ref('int9_opens_clicks_subs_by_date')}}
),

campaign_data_by_date as (
    select 
    opens_clicks_subscribers.Campaign_Date,
    CASE WHEN NAME ilike '%pop-up%' or NAME ilike '%olympics%' THEN 'Special Edition' ELSE 'Regular' END Newsletter_Type,
    sum(sends_subscribers_unsubs.Total_Sends) Sends,
    sum(sends_subscribers_unsubs.Total_Bounced) Bounces,
    sum(sends_subscribers_unsubs.Delivered_Emails) Delivered,
    sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers_unsubs.Delivered_Emails END) Total_Dojo_Sends,
    sum(sends_subscribers_unsubs.Delivered_Emails) - sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers_unsubs.Delivered_Emails END) Total_Regular_Sends,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Gmail_Total_Opens) Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Non_Gmail_Total_Opens) Non_Gmail_Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Opens) / sum(sends_subscribers_unsubs.Delivered_Emails) Total_Open_Rate,
    sum(opens_clicks_subscribers.Unique_Opens) / sum(sends_subscribers_unsubs.Delivered_Emails) Unique_Open_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails) Total_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails)  Unique_Click_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_CTOR,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR,
    SUM(sends_subscribers_unsubs.total_unsubscribes) Total_Unsubscribes,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(sends_subscribers_unsubs.Delivered_Emails) Unsubscribe_Rate,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(opens_clicks_subscribers.Unique_Opens) Unsubscribe_per_Open,
    SUM(opens_clicks_subscribers.total_partner_clicks) total_partner_clicks,
    SUM(opens_clicks_subscribers.unique_partner_clicks) unique_partner_clicks
    --sum(opens_clicks_subscribers.Total_Partner_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails) Total_Partner_Click_Rate,
    --sum(opens_clicks_subscribers.Unique_Partner_Clicks) / sum(sends_subscribers_unsubs.Delivered_Emails)  Unique_Partner_Click_Rate,
    --sum(opens_clicks_subscribers.Total_Partner_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_Partner_CTOR,
    --sum(opens_clicks_subscribers.Unique_Partner_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_Partner_CTOR
    from sends_subscribers_unsubs 
    LEFT JOIN opens_clicks_subscribers using (Campaign_ID, Name, Campaign_Date)
    GROUP BY 1,2
)

select *
from campaign_data_by_date
WHERE Campaign_Date > dateadd(month, -1, GETDATE())
ORDER BY 1