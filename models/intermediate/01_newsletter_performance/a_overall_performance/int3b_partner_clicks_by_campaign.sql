with PARTNER_CLICKS as (
    select * from {{ref('int3_partner_clicks')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

partner_clicks_by_campaign as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        PARTNER_CLICKS.*
    from CAMPAIGN_DETAILS
LEFT JOIN partner_clicks using (Campaign_ID)
)

select * from partner_clicks_by_campaign