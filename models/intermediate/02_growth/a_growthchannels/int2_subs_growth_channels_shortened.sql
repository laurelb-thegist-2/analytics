with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_int_bucket as (
    select * from {{ref('int1_growth_int_bucket')}}
),

subs_growth_int_bucket as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        growth_int_bucket.Growth_Int_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    LEFT JOIN growth_int_bucket using (EMAIL)
)

select * from subs_growth_int_bucket