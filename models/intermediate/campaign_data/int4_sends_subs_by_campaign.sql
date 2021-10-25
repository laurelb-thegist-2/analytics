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
        coalesce(subscribers.Country, 'US') Country,
        count(distinct sends.email) Total_Sends
    from sends
    LEFT JOIN subscribers using (email)
    WHERE CAMPAIGN_DATE = '2021-10-18'
    GROUP BY 1, 2, 3, 4
    ORDER BY CAMPAIGN_DATE
)

SELECT * FROM sends_subscribers
limit 1000000