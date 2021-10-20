with sends_subscribers as (
    select * 
    from {{ref('int4_sends_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select * 
    from {{ref('int7_opens_clicks_subs_by_campaign')}}
),

campaign_data_by_date as (
    select 
    opens_clicks_subscribers.Campaign_Date,
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
    sum(opens_clicks_subscribers.Unique_Clicks) / sum(opens_clicks_subscribers.Unique_Opens) Unique_CTOR,
    SUM(sends_subscribers.total_unsubscribes)/2 Total_Unsubscribes,
    (SUM(sends_subscribers.total_unsubscribes)/2) / sum(sends_subscribers.Total_Sends) Unsubscribe_Rate,
    (SUM(sends_subscribers.total_unsubscribes)/2) / sum(opens_clicks_subscribers.Unique_Opens) Unsubscribe_per_Open
    from opens_clicks_subscribers
    LEFT JOIN sends_subscribers using (Campaign_ID, Name, Campaign_Date, Country)
    WHERE Campaign_Date > '2021-09-30' 
    GROUP BY 1
    ORDER BY 1 DESC
)

select * from campaign_data_by_date
