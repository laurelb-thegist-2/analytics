with opens as (
    select * from {{ref('int2a_total_opens_by_campaign')}}
),

subscribers as (
    select * from {{ref('stg_subscribers')}}
),

opens_subscribers as (
    select 
        opens.CAMPAIGN_ID,
        opens.Name,
        opens.CAMPAIGN_DATE,
        coalesce(subscribers.Country, 'US') Country,
        count(opens.email) total_opens
    from opens
    LEFT JOIN subscribers using (email)
    GROUP BY 1,2,3,4
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM opens_subscribers