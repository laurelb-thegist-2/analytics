with user_level_sends_subscribers as (
    select * 
    from {{ref('int_user_level_sends_subscribers')}}
),

user_level_opens_clicks_subscribers as (
    select * 
    from {{ref('int_user_level_opens_clicks_subscribers')}}
),

campaign_data_by_country as (
    select 
    user_level_opens_clicks_subscribers.Name,
    user_level_opens_clicks_subscribers.Campaign_Date,
    user_level_opens_clicks_subscribers.Country,
    user_level_sends_subscribers.Total_Sends Total_Sends,
    user_level_opens_clicks_subscribers.Total_Opens Total_Opens,
    user_level_opens_clicks_subscribers.Unique_Opens Unique_Opens,
    user_level_opens_clicks_subscribers.Total_Clicks Total_Clicks,
    user_level_opens_clicks_subscribers.Unique_Clicks Unique_Clicks
    from user_level_opens_clicks_subscribers
    LEFT JOIN user_level_sends_subscribers using (Campaign_ID, Name, Campaign_Date, Country)
    WHERE Campaign_Date = '2021-10-15'
    ORDER BY Campaign_Date DESC
)

select * from campaign_data_by_country
