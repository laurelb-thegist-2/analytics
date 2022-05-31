with email_sends as (
    select * from {{ref('stg_email_sends')}}
),

campaign_details as (
    select * from {{ref('stg_campaign_details')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

unsubs as (
    select
        email_sends.Email,
        subscribers.Status,
        MAX(coalesce(subscribers.Growth_Channel, 'Organic/Unknown')) Growth_Channel,
        MAX(coalesce(subscribers.Country, 'US')) Country,
        MAX(coalesce(subscribers.Cities, 'None')) City,
        Max(Campaign_Details.Campaign_Date) Unsub_Date,
        count(distinct Email_sends.Campaign_ID) Sends
    from email_sends
    INNER JOIN Campaign_Details using (Campaign_ID)
    INNER JOIN subscribers using (Email)
    where status = 'Unsubscribed'
    Group by 1,2
)

select * from unsubs