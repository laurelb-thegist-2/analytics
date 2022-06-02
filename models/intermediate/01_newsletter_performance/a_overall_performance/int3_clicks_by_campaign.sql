with CLICKS as (
    select * from {{ref('int3a_all_clicks_by_campaign')}}
),

PARTNER_CLICKS as (
    select * from {{ref('int3b_partner_clicks_by_campaign')}}
),

clicks_by_campaign as (
    select
        CLICKS.*,
        PARTNER_CLICKS.EMAIL as PARTNER_CLICKS_EMAIL
    from CLICKS
LEFT JOIN PARTNER_CLICKS using (NAME, CAMPAIGN_DATE, Campaign_ID, City_of_Click, Country_Code_of_Click, Country_of_Click, timestamp, Region_of_Click, List_ID, URL, EMAIL)
)

select * from clicks_by_campaign