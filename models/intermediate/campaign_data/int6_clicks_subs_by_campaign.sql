with clicks as (
    select * from {{ref('int3_clicks_by_campaign')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

clicks_subscribers as (
    select 
        clicks.CAMPAIGN_ID,
        clicks.NAME,
        clicks.CAMPAIGN_DATE,
        coalesce(subscribers.Country, 'US') Country,
        URL,
        count(clicks.email) total_clicks,
        count(distinct clicks.email) unique_clicks
    from clicks
    LEFT JOIN subscribers using (email)
    Where CAMPAIGN_DATE = '2021-10-18'
    GROUP BY 1,2,3,4,5
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM clicks_subscribers