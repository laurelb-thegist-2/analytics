-- won't run for me in dbt on 2022-02-04, won't run consistently in snowflake (ran once, but then stopped working again). all other queries running for me in dbt, but email_events and email_sends aren't running in snowflake.

with campaign_details as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       Name,
       cast(Campaign_Date as DATE) Campaign_Date,
       Total_Emails_Sent as Total_Sends,
       Opens as Total_Opens,
       Total_Open_Rate,
       Unique_Opens,
       Unique_Open_Rate,
       Clicks,
       Subscriber_Clicks,
       Subscriber_Click_Rate,
       Total_Unsubscribes
    from {{ source('core', 'campaign_details') }}
    --WHERE NAME ilike '%sports biz%' and NAME ilike '%issue%'
    WHERE NAME ilike '%newsletter%'
)

select * from campaign_details