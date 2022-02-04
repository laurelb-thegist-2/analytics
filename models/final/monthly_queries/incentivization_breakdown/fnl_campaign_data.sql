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
        Campaign_Date,
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

select * from incent_vs_nonincent
Where Campaign_Date > '2021-12-31'
ORDER BY Campaign_Date