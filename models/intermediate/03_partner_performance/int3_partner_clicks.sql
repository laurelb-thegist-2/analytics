-- only filters for partners on or after Jan 1 2022

with partner_clicks as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       City as City_of_Click,
       CountryCode as Country_Code_of_Click,
       CountryName as Country_of_Click,
       Date as timestamp,
       lower(EmailAddress) as Email,
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
URL ilike '%babbel%' or 
URL ilike '%olliepets%' or 
URL ilike '%keepherinplay%' or
URL ilike '%policyme%' OR 
URL ilike '%golfmonthly%' or
URL ilike '%muskokabrewery%'or 
URL ilike '%thegetrealmovement%' or
URL ilike '%meritbeauty%' or 
URL ilike '%keepherinplay%' or 
URL ilike '%masbasbas%' or
URL ilike '%myollie%' or
URL ilike '%wilmajeanwrinkles%' or
URL ilike '%jack%' or
URL ilike '%future%' or
URL ilike '%futurefitness%' or 
URL ilike '%iucn%' or 
URL ilike '%parley%' or
URL ilike '%tangerine%' or
URL ilike '%bluehens%' or 
URL ilike '%thegetrealmovement%' or 
URL ilike '%foodandwine%' or 
URL ilike '%cometeer%' or 
URL ilike '%PMETyfE!KM2ziNG%' or
URL ilike '%32210623%' or 
URL ilike '%cnbc.com/2021/03/18%' or
URL ilike '%wnba-atlanta-dream-rhyne-howard%' or 
URL ilike '%76009dbf-fd50-4c67-a1c0-dbe1e1380c62%' or 
URL ilike '%moroccanoil%' or
URL ilike '%troy-polamalu-hair%' or 
URL ilike '%e2dc11b0-8c8d-429f-8fef-d23147b9bb65%' or 
URL ilike '%nba-75th-anniversary-basketball%' or
URL ilike '%unesco.org/themes/gender-equality-sports-media%'