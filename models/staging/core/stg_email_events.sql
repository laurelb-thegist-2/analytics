with email_events as (
    select 
        lower(Email) Email,
        CampaignID as Campaign_ID, 
        Action,
        Timestamp,
        URL
    from {{ source('core', 'email_events') }}
)

select * from email_events