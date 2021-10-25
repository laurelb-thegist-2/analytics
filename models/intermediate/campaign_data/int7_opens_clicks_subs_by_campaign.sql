with opens_subscribers as (
    select * from {{ref('int5_opens_subs_by_campaign')}}
),

clicks_subscribers as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

opens_clicks_subscribers as (
    select 
    opens_subscribers.CAMPAIGN_ID,
    opens_subscribers.NAME,
    opens_subscribers.CAMPAIGN_DATE,
    opens_subscribers.COUNTRY,
    opens_subscribers.Total_Opens,
    opens_subscribers.Unique_Opens,
    sum(clicks_subscribers.Total_Clicks) Total_Clicks,
    sum(clicks_subscribers.Unique_Clicks) Unique_Clicks
    from opens_subscribers
    LEFT JOIN clicks_subscribers 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY)
    WHERE CAMPAIGN_DATE = '2021-10-18'
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 1
)

select * from opens_clicks_subscribers