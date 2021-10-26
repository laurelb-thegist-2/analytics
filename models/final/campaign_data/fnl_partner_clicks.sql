with clicks as (
    select * from {{ref('int3_clicks_by_campaign')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

link_clicks as (
    select 
        clicks.CAMPAIGN_DATE,
        clicks.NAME,
        coalesce(subscribers.Country, 'US') Country,
        count(clicks.email) total_clicks,
        count(distinct clicks.email) unique_clicks
    from clicks
    LEFT JOIN subscribers using (email)
    Where CAMPAIGN_DATE = '2021-10-18' and URL ilike '%TheNextHoops%'
    GROUP BY 1,2,3
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM link_clicks