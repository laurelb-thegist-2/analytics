-- pulls from int2a_total_opens_by_campaign and int2b_unique_opens_by_campaign (found under folder campaign_data)
-- int2a combines campaign_opens with campaign_details and filters out boardman opens from the total opens
-- int2b combines email_events with campaign_details and does not filter out boardman opens from unique opens

with total_opens as (
    select * from {{ref('int2a_total_opens_by_campaign')}}
),

unique_opens as (
    select * from {{ref('int2b_unique_opens_by_campaign')}}
),

opens as (
    select 
        total_opens.Email,
        total_opens.Campaign_ID,
        total_opens.Campaign_Date,
        total_opens.Name,
        MIN(total_opens.Timestamp) First_Open,
        Count(total_opens.email) Total_Opens,
        count(distinct unique_opens.email) Unique_Opens
    from total_opens
    LEFT JOIN unique_opens using (Campaign_ID, Campaign_Date, Email, Name)
    GROUP BY 1, 2, 3, 4
)

select * from opens

