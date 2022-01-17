with opens as (
    select * from {{ref('int5c_opens_subs_by_campaign')}}
),

clicks as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

opens_clicks as (
    select 
    opens.CAMPAIGN_ID,
    opens.NAME,
    opens.CAMPAIGN_DATE,
    opens.COUNTRY,
    opens.Total_Opens,
    opens.Unique_Opens,
    clicks.Total_Clicks,
    clicks.Unique_Clicks
    from opens
    LEFT JOIN clicks 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY)
)

select * from opens_clicks
WHERE CAMPAIGN_DATE IS NOT NULL
ORDER BY CAMPAIGN_DATE DESC