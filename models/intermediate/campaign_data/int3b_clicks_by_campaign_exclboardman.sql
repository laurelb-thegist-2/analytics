with clicks_by_campaign as (
    select * from {{ref('int3a_clicks_by_campaign')}}
),

campaign_clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

clicks_by_campaign_exclboardman as (
    select 
        clicks_by_campaign.CAMPAIGN_ID,
        clicks_by_campaign.email,
        clicks_by_campaign.NAME,
        clicks_by_campaign.CAMPAIGN_DATE,
        campaign_clicks.City_of_Click,
        clicks_by_campaign.URL
    from clicks_by_campaign
    INNER JOIN campaign_clicks using (email, campaign_ID, URL, timestamp)
)

SELECT * FROM clicks_by_campaign_exclboardman
where campaign_date = '2021-11-28'
--city_of_click != 'Boardman'
limit 10000