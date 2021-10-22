with click_subscribers as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

partner_clicks as ( 
    select 
        Campaign_Date,
        Country,
        sum(Total_Clicks) Total_Clicks,
        sum(Unique_Clicks) Unique_Clicks
    from click_subscribers
    where URL ilike '%penguin%'
    AND Campaign_Date ilike '%2021-10-19%'
    GROUP BY 1, 2
    ORDER BY 1 DESC
)

select * from partner_clicks