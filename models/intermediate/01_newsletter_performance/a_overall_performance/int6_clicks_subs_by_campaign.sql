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
        coalesce(subscribers.Cities, 'None') City,
        coalesce(subscribers.Growth_Channel,'Organic/Unknown') Growth_Channel,
        count(clicks.email) total_clicks,
        count(distinct clicks.email) unique_clicks
    from clicks
    LEFT JOIN subscribers using (email)
    GROUP BY 1,2,3,4,5,6
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM clicks_subscribers