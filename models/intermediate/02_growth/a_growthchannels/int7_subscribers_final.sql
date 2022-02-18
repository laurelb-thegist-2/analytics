-- final table has all subscribers with their growth channel, their shortened and highest level growth channels, their 
-- churn type (e.g., voluntary, non-voluntary and bounced) and their incent type (e.g., incent vs unincent)
-- this is the final subscriber query that should be used in all other queries 

with subscribers as (
    select * from {{ref('int6_subs_churn_type')}}
),

incentivized as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        CASE WHEN Growth_Bucket ilike '%dojo%' or Growth_Bucket ilike '%contest%' THEN 'Incentivized' END Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    WHERE Growth_Bucket ilike '%dojo%' or Growth_Bucket ilike '%contest%'
), 

unincentivized as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        CASE WHEN Growth_Bucket not ilike '%dojo%' and Growth_Bucket not ilike '%contest%' or Growth_Bucket IS NULL THEN 'Unincentivized' END Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    WHERE Growth_Bucket not ilike '%dojo%' and Growth_Bucket not ilike '%contest%' or Growth_Bucket IS null
), 

final_subscribers as (
    select * from incentivized
    union
    select * from unincentivized
)

select * from final_subscribers
