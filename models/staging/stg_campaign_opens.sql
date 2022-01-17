with campaign_opens as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Open,
       CountryCode as Country_Code_of_Open,
       CountryName as Country_of_Open,
       Date as timestamp,
       EmailAddress as Email,
       Region as Region_of_Open
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_opens
    where city_of_open != 'Boardman' or city_of_open is NULL
)

select * from campaign_opens