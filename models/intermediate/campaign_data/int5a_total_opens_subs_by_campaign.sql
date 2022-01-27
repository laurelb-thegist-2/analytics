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
        coalesce(subscribers.Cities, 'None') City,
        coalesce(subscribers.Growth_Channel, 'Organic/Unknown') Growth_Channel, 
        count(opens.email) total_opens
    from opens
    LEFT JOIN subscribers using (email)
    GROUP BY 1,2,3,4,5,6
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM opens_subscribers