with opens as (
    select * from {{ref('int5_opens_subs_by_campaign')}}
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
        coalesce(Growth_Channel, 'Organic/Unknown') Growth_Channel, 
        coalesce(Growth_Bucket, 'Organic/Unknown') Growth_Bucket,
        coalesce(Incentivization, 'Unincentivized') Incentivization,
        sends.Delivered_Emails Delivered,
        opens.Total_Opens Total_Opens,
        opens.Unique_Opens Unique_Opens,
        clicks.Total_Clicks Total_Clicks,
        clicks.Unique_Clicks Unique_Clicks
    from sends
    FULL OUTER JOIN opens using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, CITY, Growth_Channel, Growth_Bucket, Incentivization)
    FULL OUTER JOIN clicks using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, CITY, Growth_Channel, Growth_Bucket, Incentivization)
)

select * from campaign_data
Where Campaign_Date > '2021-12-31'
ORDER BY Campaign_Date