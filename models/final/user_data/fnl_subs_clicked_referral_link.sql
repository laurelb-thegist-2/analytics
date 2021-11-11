with SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
),

email_events as (
    select * from {{ref('stg_email_events')}}
),

subs_email_events as (
    select
        SUBSCRIBERS.email,
        SUBSCRIBERS.status,
        SUBSCRIBERS.country,
        SUBSCRIBERS.referral_code,
        SUBSCRIBERS.referral_count,
        email_events.Action,
        email_events.URL
    from SUBSCRIBERS
LEFT JOIN email_events using (email)
WHERE status = 'Active' and Action ilike '%Click%' and URL ilike '%thegistsports.com/referral%'
)

select * from subs_email_events

