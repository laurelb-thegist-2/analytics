with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_channels as (
    select * from {{ref('int3_growth_channels')}}
),

final_subscribers as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Growth_Channel,
        growth_channels.Growth_Int_Bucket,
        growth_channels.Growth_Bucket,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    LEFT JOIN growth_channels using (EMAIL)
)

select * from final_subscribers
where status = 'Active'
limit 400000
