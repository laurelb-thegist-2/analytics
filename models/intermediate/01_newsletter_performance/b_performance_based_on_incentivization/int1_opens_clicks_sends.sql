with opens as (
    select * from {{ref('int5c_opens_subs_by_campaign')}}
),

clicks as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

sends as (
    select * from {{ref('int4_sends_subs_by_campaign')}}
),

campaign_data as (
    select
        Campaign_ID,
        Name,
        Campaign_Date,
        Country,
        City,
        Growth_Channel,
        sends.Delivered_Emails Delivered,
        opens.Total_Opens Total_Opens,
        opens.Unique_Opens Unique_Opens,
        clicks.Total_Clicks Total_Clicks,
        clicks.Unique_Clicks Unique_Clicks
    from sends
    FULL OUTER JOIN opens using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, CITY, Growth_Channel)
    FULL OUTER JOIN clicks using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, CITY, Growth_Channel)
)

select * from campaign_data
Where Campaign_Date > '2021-12-31'
ORDER BY Campaign_Date