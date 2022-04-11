-- only filters for partners on or after Jan 1 2022

with partner_clicks as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Click,
       CountryCode as Country_Code_of_Click,
       CountryName as Country_of_Click,
       Date as timestamp,
       lower(EmailAddress) as Partner_Clicks_Email,
       Region as Region_of_Click,
       LISTID as List_ID,
       URL
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_clicks
    where URL is not NULL and (city_of_click != 'Boardman' or city_of_click is NULL) 
    and list_id = '54eb7610971ecdad5354d8d07b2b6397' 
)

select * from partner_clicks
where 
URL ilike '%ad.doubleclick%' or
URL ilike '%fanduel%' or
URL ilike '%gistbrand%' or
URL ilike '%21seeds%' or
URL ilike '%athleta%' or
URL ilike '%75thshop%' or
URL ilike '%thrivecausemetics%' or
URL ilike '%butcherbox%' or
URL ilike '%9NZvM1918_E%' or
URL ilike '%olympicchannel-chloe-kim-AuEKTqUjj6KjBeOGs5%' or
URL ilike '%clorox%' or
URL ilike '%hockeycanada%' or
URL ilike '%reel/CZ0PGuGFCia%' or
URL ilike '%athleta%' or
URL ilike '%sponsorpulse%' or
URL ilike '%underarmour%' or
URL ilike '%canadiantire%' or 
URL ilike '%aAsqS_HYfE4&t=11s%' or
URL ilike '%sportchek%' or
URL ilike '%ontariosportnetwork%' or 
URL ilike '%theplayerstribune%' or
URL ilike '%campaignmonitor%' or
URL ilike '%math-lady-confused-lady%' or
URL ilike '%lmnt%'or 
URL ilike '%babbel%' or 
URL ilike '%adidas%' or
URL ilike '%cupofte%' or 
URL ilike '%the-first-raptors-star%' or
URL ilike '%babbel%'