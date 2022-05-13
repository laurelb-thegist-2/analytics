with email_sends as (
    select 
        CampaignID as Campaign_ID,
        lower(Email) Email
    from {{ source('core', 'email_sends') }}
)

select * from email_sends