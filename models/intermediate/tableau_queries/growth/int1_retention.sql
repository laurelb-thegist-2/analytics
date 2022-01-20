-- pulls from int1_sends_by_campaign (found in folder campaign_data) and it combines email_events and campaign_details

with sends as (
    select * from {{ref('int1_sends_by_campaign')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

retention as (
    select
        sends.Email,
        subscribers.Growth_Channel,
        subscribers.Status,
        coalesce(subscribers.Country, 'US') Country,
        coalesce(subscribers.Cities, 'None') City,
        MIN(sends.CAMPAIGN_DATE) First_Email,
        MAX(sends.CAMPAIGN_DATE) Last_Email,
        DATEDIFF(day, First_Email, Last_Email) as Days_Retained,
        count(distinct sends.CAMPAIGN_ID) Total_Campaigns
    from sends
    LEFT JOIN subscribers using (email)
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY FIRST_EMAIL
)

SELECT * FROM retention