-- pulls from int2_opens_by_campaign and int2b_unique_opens_by_campaign (found under folder campaign_data), which combines campaign_opens with campaign_details and filters out boardman opens from the total opens

with opens as (
    select * from {{ref('int2_opens_by_campaign')}}
),


opens_by_user as (
    select 
        Email,
        Campaign_ID,
        Campaign_Date,
        Name,
        MIN(Timestamp) First_Open,
        MAX(Timestamp) Most_Recent_Open,
        count(CASE WHEN City_of_Open != 'Boardman' or city_of_open is NULL THEN email END) Total_Opens,
        count(distinct email) Unique_Opens
    from opens
    GROUP BY 1, 2, 3, 4
)

select * from opens_by_user

