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
    sum(sends_subscribers_unsubs.Total_Sends) Total_Sends,
    sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers_unsubs.Total_Sends END) Total_Dojo_Sends,
    sum(sends_subscribers_unsubs.Total_Sends) - sum(CASE WHEN opens_clicks_subscribers.Name ILIKE '%DOJO%' THEN sends_subscribers_unsubs.Total_Sends END) Total_Regular_Sends,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Opens) / sum(sends_subscribers_unsubs.Total_Sends) Total_Open_Rate,
    sum(opens_clicks_subscribers.Unique_Opens) / sum(sends_subscribers_unsubs.Total_Sends) Unique_Open_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(sends_subscribers_unsubs.Total_Sends) Total_Click_Rate,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(sends_subscribers_unsubs.Total_Sends)  Unique_Click_Rate,
    sum(opens_clicks_subscribers.Total_Clicks) / sum(opens_clicks_subscribers.Total_Opens) Total_CTOR,
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR,
    SUM(sends_subscribers_unsubs.total_unsubscribes) Total_Unsubscribes,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(sends_subscribers_unsubs.Total_Sends) Unsubscribe_Rate,
    SUM(sends_subscribers_unsubs.total_unsubscribes) / sum(opens_clicks_subscribers.Unique_Opens) Unsubscribe_per_Open
    from sends_subscribers_unsubs 
    LEFT JOIN opens_clicks_subscribers using (Campaign_ID, Name, Campaign_Date)
    GROUP BY 1
)

select * from campaign_data_by_date
WHERE Campaign_Date is not null and Campaign_Date > '2021-12-31'
ORDER BY Campaign_Date
