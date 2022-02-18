with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_bucket as (
    select * from {{ref('int3_growth_bucket')}}
),

final_subscribers as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        growth_bucket.Growth_Int_Bucket,
        growth_bucket.Growth_Bucket,
        Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    LEFT JOIN growth_bucket using (EMAIL)
)

select * from final_subscribers
