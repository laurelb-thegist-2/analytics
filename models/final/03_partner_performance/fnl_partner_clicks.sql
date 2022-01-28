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
    where URL ilike '%FANDUEL%' --or URL ilike '%wilson-castaway.gif' or URL ilike '%car-oprah-winfrey%' 
    and campaign_date = '2022-01-24'
    GROUP BY 1,2--,3
)

SELECT * FROM partner_clicks