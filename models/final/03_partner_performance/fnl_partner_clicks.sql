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
    where URL ilike '%future%' --or URL ilike '%NIK-07%' --line to change, where you add URL search term
    GROUP BY 1,2--,3
)

SELECT * FROM partner_clicks
WHERE CAMPAIGN_DATE = '2022-05-20' --line to change, this is where you add the campaign date