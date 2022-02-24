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
        coalesce(Deleted.Deleted_Volume, 0) as Deleted_Volume,
        Deleted.Deleted_Unique_Open_Rate,
        Deleted.Deleted_Click_Rate,
        Deleted.Deleted_Total_CTOR,
        coalesce(Bounced.Bounced_Volume, 0) as Bounced_Volume,
        Bounced.Bounced_Unique_Open_Rate,
        Bounced.Bounced_Click_Rate,
        Bounced.Bounced_Total_CTOR,
        sum(Active_Volume) / (Active_Volume + Unsubs_Volume + Deleted_Volume + Unsubs_Volume) as Retention
    from active 
    LEFT JOIN Unsubs using (Growth_Bucket, Incentivization)
    LEFT JOIN deleted using (Growth_Bucket, Incentivization)
    LEFT JOIN bounced using (Growth_Bucket, Incentivization)
)

select * from all_subs

