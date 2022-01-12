with campaign_clicks as (
    select * from {{ref('stg_campaign_clicks')}}
),

CAMPAIGN_DETAILS as (
    select * from {{ref('stg_campaign_details')}}
),

CLICK_SUMMARY as (
    SELECT EMAIL,
        MIN(CAMPAIGN_DATE) FIRST_CLICK,
        MAX(CAMPAIGN_DATE) MOST_RECENT_CLICK,
        count(distinct campaign_clicks.Campaign_ID) CAMPAIGNS_CLICKED,
        count(campaign_clicks.URL) CLICKS
    from campaign_clicks 
    LEFT JOIN CAMPAIGN_DETAILS using (Campaign_ID) 
    WHERE campaign_clicks.URL is not null
    GROUP BY 1
)

select * from CLICK_SUMMARY

