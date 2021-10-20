with sends as (
    select * from {{ref('int1_sends_by_campaign')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

sends_subscribers as (
    select
        sends.CAMPAIGN_ID, 
        sends.Name,
        sends.CAMPAIGN_DATE,
        subscribers.Country,
        sends.total_unsubscribes,
        count(sends.email) total_sends
    from sends
    LEFT JOIN subscribers using (email)
    GROUP BY 1,2,3,4,5
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM sends_subscribers