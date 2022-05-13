with campaign_bounces as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       Date as timestamp,
       lower(EmailAddress) as Email,
       LISTID as List_ID
    from analytics.CAMPAIGN_MONITOR_EVENTS.campaign_bounces
    where list_id = '54eb7610971ecdad5354d8d07b2b6397' --sports news
    --where list_id = '65ef2f913391ff42878e99dd01601196' --sports biz
)

select * from campaign_bounces