-- final table has all subscribers with their growth channel, their shortened and highest level growth channels, their 
-- churn type (e.g., voluntary, non-voluntary and bounced) and their incent type (e.g., incent vs unincent)
-- this is the final subscriber query that should be used in all other queries 

with subscribers as (
    select * from {{ref('int6_subs_churn_type')}}
),

incent as (
    select * from {{ref('int7_incent')}}
),

final_subscribers as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        Type_of_Churn,
        Growth_Channel,
        Growth_Int_Bucket,
        Growth_Bucket,
        incent.Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand
    from subscribers
    LEFT JOIN incent using (EMAIL)
)

select * from final_subscribers
