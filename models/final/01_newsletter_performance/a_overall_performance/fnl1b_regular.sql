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
    CASE WHEN NAME not ilike '%pop-up%' and NAME not ilike '%olympics%' THEN 'Regular' END Newsletter_Type,
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
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(opens_clicks_subscribers.Unique_Opens) Unsubscribe_per_Open
    from sends_subscribers_unsubs 
    LEFT JOIN opens_clicks_subscribers using (Campaign_ID, Name, Campaign_Date)
    WHERE NAME not ilike '%pop-up%' and NAME not ilike '%olympics%'
    GROUP BY 1,2
)

select * from campaign_data_by_date
WHERE Campaign_Date > '2021-12-31'
ORDER BY 1