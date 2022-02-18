with subscribers as (
    select * from {{ref('int4_subs_growth_channel')}}
),

voluntary as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        CASE WHEN status ILIKE '%Unsubscribed%' THEN 'Voluntary' END Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    WHERE Status ilike '%Unsubscribed%'
), 

non_voluntary as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        CASE WHEN status ILIKE '%Deleted%' THEN 'Non-voluntary' END Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    WHERE Status ilike '%Deleted%'
), 

bounced as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        status,
        CASE WHEN status ILIKE '%Bounced%' THEN 'Bounced' END Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    WHERE Status ilike '%Bounced%'
), 

subs_type_of_churn as (
    select * from voluntary
    union
    select * from non_voluntary
    union
    select * from bounced
)

select * from subs_type_of_churn
