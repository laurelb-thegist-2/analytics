with active as (
    select * from {{ref('fnl_active_subs')}}
),

Bounced as (
    select * from {{ref('fnl_bounced_subs')}}
),

Deleted as (
    select * from {{ref('fnl_deleted_subs')}}
),

Unsubs as (
    select * from {{ref('fnl_unsubs')}}
),

all_subs as ( 
    select 
        Growth_Bucket,
        Incentivization, 
        coalesce(active.Active_Volume, 0) as Active_Volume,
        active.Active_Unique_Open_Rate,
        active.Active_Click_Rate,
        active.Active_Total_CTOR,
        coalesce(Unsubs.Unsubs_Volume, 0) as Unsubs_Volume,
        Unsubs.Unsubs_Unique_Open_Rate,
        Unsubs.Unsubs_Click_Rate,
        Unsubs.Unsubs_Total_CTOR,
        coalesce(Bounced.bounced_Volume, 0) as Bounced_Volume,
        Bounced.bounced_Unique_Open_Rate,
        Bounced.bounced_Click_Rate,
        Bounced.bounced_Total_CTOR,
        coalesce(Deleted.deleted_Volume, 0) as Deleted_Volume,
        Deleted.deleted_Unique_Open_Rate,
        Deleted.deleted_Click_Rate,
        Deleted.deleted_Total_CTOR
    from active 
    LEFT JOIN Unsubs using (Growth_Bucket, Incentivization)
    LEFT JOIN deleted using (Growth_Bucket, Incentivization)
    LEFT JOIN bounced using (Growth_Bucket, Incentivization)
    --GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
)

select * from all_subs

