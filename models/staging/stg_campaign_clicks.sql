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
)

select * from campaign_clicks
where city_of_click != 'Boardman' and Campaign_ID = 'f7464ac18168ef72f30fbc6af76e164c'
limit 10000
