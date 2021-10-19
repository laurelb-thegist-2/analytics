with sends_subscribers as (
    select * 
    from {{ref('int4_sends_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select * 
    from {{ref('int7_opens_clicks_subs_by_campaign')}}
),

campaign_data_summary as (
    select 
    opens_clicks_subscribers.Campaign_Date,
    sum(sends_subscribers.Total_Sends) Total_Sends,
    sum(opens_clicks_subscribers.Total_Opens) Total_Opens,
    sum(opens_clicks_subscribers.Unique_Opens) Unique_Opens,
    sum(opens_clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(opens_clicks_subscribers.Unique_Clicks) Unique_Clicks
    from opens_clicks_subscribers
    LEFT JOIN sends_subscribers using (Campaign_ID, Name, Campaign_Date, Country)
    WHERE Campaign_Date > '2021-10-15'
    GROUP BY 1
    ORDER BY Campaign_Date DESC
)

select * from campaign_data_summary
