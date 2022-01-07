with campaign_clicks_details_subs as (
    select * from {{ref('int2_campaign_clicks_details_subs')}}
),

partner_clicks as (
    select 
        Campaign_Date,
        --URL,
        Country,
        count(email) total_clicks,
        count(distinct email) unique_clicks
    from campaign_clicks_details_subs
    where City_of_Click != 'Boardman'
    and campaign_date = '2022-01-05' 
    and URL ilike '%ad.doubleclick%' 
    GROUP BY 1, 2
)

SELECT * FROM partner_clicks
limit 100000