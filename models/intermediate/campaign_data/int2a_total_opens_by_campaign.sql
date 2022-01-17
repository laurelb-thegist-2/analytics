with OPENS as (
    select * from {{ref('stg_campaign_opens')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

opens_by_campaign as (
    select
        Campaign_Details.NAME,
        Campaign_Details.CAMPAIGN_DATE,
        OPENS.*
    from OPENS
LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID)
)

select * from opens_by_campaign