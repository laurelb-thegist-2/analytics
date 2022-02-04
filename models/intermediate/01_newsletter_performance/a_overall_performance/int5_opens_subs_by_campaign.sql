with opens as (
    select * from {{ref('int2_opens_by_campaign')}}
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
        count(CASE WHEN opens.City_of_Open != 'Boardman' or opens.city_of_open is NULL THEN opens.email END) Total_Opens,
        count(distinct opens.email) Unique_Opens,
        count(CASE WHEN opens.email ilike '%gmail%' THEN opens.email END) Gmail_Total_Opens,
        count(CASE WHEN opens.email not ilike '%gmail%' THEN opens.email END) Non_Gmail_Total_Opens
    from opens
    LEFT JOIN subscribers using (email)
    GROUP BY 1,2,3,4,5,6
    ORDER BY CAMPAIGN_DATE DESC
)

SELECT * FROM opens_subscribers