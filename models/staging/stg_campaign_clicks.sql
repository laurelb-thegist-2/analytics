with campaign_clicks as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Click,
       CountryCode as Country_Code_of_Click,
       CountryName as Country_of_Click,
       Date as timestamp,
       EmailAddress as Email,
       Region as Region_of_Click,
       URL
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_clicks
    --WHERE NAME ilike '%%'
)

select * from campaign_clicks
where Campaign_ID = '36ae63be68f3f881fd8aaf86c4754b2e' or Campaign_ID = '976ebd461e0d7750369cf9a95731c145'
limit 10000
