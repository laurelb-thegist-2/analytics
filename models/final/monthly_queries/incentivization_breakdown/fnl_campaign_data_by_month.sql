with opens as (
    select * from {{ref('int5_opens_subs_by_campaign')}}
),

clicks as (
    select * from {{ref('int6_clicks_subs_by_campaign')}}
),

sends as (
    select * from {{ref('int4_sends_subs_by_campaign')}}
),

incent_vs_nonincent as (
    select
        date_trunc('month', Campaign_Date) Month,
        count(distinct CASE WHEN NAME ilike '%pop%' THEN Campaign_Date END) + count(distinct CASE WHEN NAME not ilike '%pop%' or NAME is null THEN Campaign_Date END) Number_of_Newsletters,
        sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN sends.Total_Sends END) Incent_Sends,
        sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN sends.Total_Sends END) Non_Incent_Sends,
        sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN opens.Total_Opens END) Incent_Total_Opens,
        sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN opens.Total_Opens END) Non_Incent_Total_Opens,
        sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN opens.Unique_Opens END) Incent_Unique_Opens,
        sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN opens.Unique_Opens END) Non_Incent_Unique_Opens,
        (sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN opens.Unique_Opens END)/sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN sends.Total_Sends END)) Incent_UOR,
        (sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN opens.Unique_Opens END)/sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN sends.Total_Sends END)) Non_Incent_UOR,
        sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN clicks.Total_Clicks END) Incent_Total_Clicks,
        sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN clicks.Total_Clicks END) Non_Incent_Total_Clicks,
        sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN clicks.Unique_Clicks END) Incent_Unique_Clicks,
        sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN clicks.Unique_Clicks END) Non_Incent_Unique_Clicks,
        (sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN clicks.Total_Clicks END)/sum(CASE WHEN (Growth_Channel ilike '%coreg%' or Growth_Channel ilike '%contest%') THEN opens.Total_Opens END)) Incent_Total_CTOR,
        (sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN clicks.Total_Clicks END)/sum(CASE WHEN (Growth_Channel not ilike '%coreg%' and Growth_Channel not ilike '%contest%') or Growth_Channel is null THEN opens.Total_Opens END)) Non_Incent_Total_CTOR
    from sends
    LEFT JOIN opens using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, Growth_Channel)
    LEFT JOIN clicks using (Campaign_ID, Name, CAMPAIGN_DATE, COUNTRY, Growth_Channel)
    GROUP BY 1
)

select
    Month,
    Number_of_Newsletters,
    round(Incent_Sends / Number_of_Newsletters , 0) Avg_Incent_Sends,
    round(Non_Incent_Sends / Number_of_Newsletters , 0) Avg_Non_Incent_Sends,
    round(Incent_Total_Opens / Number_of_Newsletters , 0) Avg_Incent_Total_Opens,
    round(Non_Incent_Total_Opens / Number_of_Newsletters , 0) Avg_Non_Incent_Total_Opens,
    round(Incent_Unique_Opens / Number_of_Newsletters , 0) Avg_Incent_Unique_Opens,
    round(Non_Incent_Unique_Opens / Number_of_Newsletters , 0) Avg_Non_Incent_Unique_Opens,
    (Incent_Unique_Opens / Number_of_Newsletters) / (Incent_Sends / Number_of_Newsletters) Avg_Incent_UOR,
    (Non_Incent_Unique_Opens / Number_of_Newsletters) / (Non_Incent_Sends / Number_of_Newsletters) Avg_Non_Incent_UOR,
    round(Incent_Total_Clicks / Number_of_Newsletters , 0) Avg_Incent_Total_Clicks,
    round(Non_Incent_Total_Clicks / Number_of_Newsletters , 0) Avg_Non_Incent_Total_Clicks,
    round(Incent_Unique_Clicks / Number_of_Newsletters , 0) Avg_Incent_Unique_Clicks,
    round(Non_Incent_Unique_Clicks / Number_of_Newsletters , 0) Avg_Non_Incent_Unique_Clicks,
    (Incent_Total_Clicks / Number_of_Newsletters) / (Incent_Total_Opens / Number_of_Newsletters) Avg_Incent_CTOR,
    (Non_Incent_Total_Clicks / Number_of_Newsletters) / (Non_Incent_Total_Opens / Number_of_Newsletters) Avg_Non_Incent_CTOR
from incent_vs_nonincent
Where Month > '2021-12-31'
ORDER BY Month