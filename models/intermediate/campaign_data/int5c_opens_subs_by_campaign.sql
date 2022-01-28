with total_opens as (
    select * from {{ref('int5a_total_opens_subs_by_campaign')}}
),

unique_opens as (
    select * from {{ref('int5b_unique_opens_subs_by_campaign')}}
),

opens as (
    select 
        total_opens.CAMPAIGN_ID,
        total_opens.NAME,
        total_opens.CAMPAIGN_DATE,
        total_opens.COUNTRY,
        total_opens.City,
        total_opens.Growth_Channel, 
        total_opens.Total_Opens,
        total_opens.Gmail_Total_Opens,
        total_opens.Non_Gmail_Total_Opens,
        unique_opens.Unique_Opens
    from total_opens
    LEFT JOIN unique_opens 
    USING (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, City, Growth_Channel)
)

select * from opens
WHERE CAMPAIGN_DATE IS NOT NULL
ORDER BY CAMPAIGN_DATE DESC