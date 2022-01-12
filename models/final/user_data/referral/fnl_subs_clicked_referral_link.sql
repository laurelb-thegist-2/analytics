with SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

campaign_clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

subs_campaign_clicks as (
    select
        SUBSCRIBERS.email,
        SUBSCRIBERS.status,
        SUBSCRIBERS.country,
        SUBSCRIBERS.referral_code,
        SUBSCRIBERS.referral_count,
        campaign_clicks.URL
    from SUBSCRIBERS
LEFT JOIN campaign_clicks using (email)
WHERE status = 'Active' and URL ilike '%thegistsports.com/referral%'
)

select * from subs_campaign_clicks

