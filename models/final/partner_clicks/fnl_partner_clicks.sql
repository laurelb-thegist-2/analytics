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
    where Campaign_Date = '2022-01-16' and URL ilike '%ad.doubleclick%'
    GROUP BY 1,2--,3
)

SELECT * FROM partner_clicks