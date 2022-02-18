with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_channels_shortened as (
    select * from {{ref('int1_growth_channels_shortened')}}
),

subs_growth_channels_shortened as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        growth_channels_shortened.Growth_Int_Bucket,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    LEFT JOIN growth_channels_shortened using (EMAIL)
)

select * from subs_growth_channels_shortened