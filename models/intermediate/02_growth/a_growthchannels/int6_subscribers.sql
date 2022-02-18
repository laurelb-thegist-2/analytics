-- final table has all subscribers with their growth channel, their shortened and highest level growth channels, their 
-- churn type (e.g., voluntary, non-voluntary and bounced) and their incent type (e.g., incent vs unincent)
-- this is the final subscriber query that should be used in all other queries 

with subscribers as (
    select * from {{ref('int4_subs_growth_channel')}}
),

churn_type as (
    select * from {{ref('int5_type_of_churn')}}
),

final_subscribers as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        churn_type.Type_of_Churn,
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
    LEFT JOIN churn_type using (EMAIL)
)

select * from final_subscribers
