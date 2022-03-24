with campaign_clicks_details_subs as (
    select * from {{ref('int2_campaign_clicks_details_subs')}}
),

partner_clicks as (
    select 
        Campaign_Date,
        Country,
        --URL,
        count(email) total_clicks,
        count(distinct email) unique_clicks
    from campaign_clicks_details_subs
    where URL ilike '%canadiantire%' or URL ilike '%aAsqS_HYfE4&t=11s%' or URL ilike '%sportchek%' or URL ilike '%sportchek%' or URL ilike '%theplayerstribune%' 
    GROUP BY 1,2--,3
)

SELECT * FROM partner_clicks
WHERE CAMPAIGN_DATE = '2022-03-06'