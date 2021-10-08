with campaign_details as ( 
    select 
       CAMPAIGNID AS Campaign_ID,
       Name,
       Campaign_Date,
       Total_Emails_Sent as Total_Sends,
       Opens as Total_Opens,
       Total_Open_Rate,
       Unique_Opens,
       Unique_Open_Rate,
       Subscriber_Clicks as Total_Clicks,
       Subscriber_Click_Rate,
       Total_Unsubscribes
    from analytics.core.campaign_details
)

select * from campaign_details