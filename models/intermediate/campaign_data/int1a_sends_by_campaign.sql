-- trying to subtract bounces from sends to get delivered emails. not there yet, bounces won't show up as a column. come back to later.

with sends as (
    select * from {{ref('int1_sends_by_campaign')}}
),

CAMPAIGN_BOUNCES as (
    select * from {{ref('stg_campaign_bounces')}}
),

sends as (
    select
        CAMPAIGN_DATE,
        Campaign_ID,
        NAME,
        Email as Sent,
        CASE WHEN CAMPAIGN_BOUNCES.Email IS NOT NULL THEN 1 ELSE 0 END as Bounces
    from sends 
    LEFT JOIN CAMPAIGN_BOUNCES using (Campaign_ID, Email)
)

select * from sends