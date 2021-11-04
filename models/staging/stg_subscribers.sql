with subscribers as ( 
    select 
        EMAILADDRESS AS EMAIL,
        LISTID as list_ID,
        cast("DATE" as DATE) as date_status_changed,
        STATUS,
        GROWTHCHANNEL as growth_channel,
        coalesce(Country, 'US') Country,
        CITIES,
        REFERRALCODE as referral_code,
        REFERRALCOUNT as referral_count,
        CAMPAIGNNAME as campaign_name,
        SOURCEBRAND as source_brand
    from analytics.core.all_subscribers 
    where list_id = '54eb7610971ecdad5354d8d07b2b6397'
    order by 3 desc
)

select * from subscribers