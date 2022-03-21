-- only filters for partners on or after Jan 1 2022

with clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

partner_clicks as (
    select
        Email,     
        Campaign_ID,
        URL,
        Timestamp
    from clicks
    WHERE 
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
    URL ilike '%lmnt%'
)   

select * from partner_clicks