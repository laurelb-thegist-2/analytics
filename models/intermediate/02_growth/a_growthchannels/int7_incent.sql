with subscribers as (
    select * from {{ref('int6_subscribers')}}
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

incentivization as (
    select * from incentivized
    union
    select * from unincentivized
)

select * from incentivized
