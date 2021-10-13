with subscribers as ( 
    select 
        EMAILADDRESS AS EMAIL,
        LISTID as list_ID,
        DATE as date_status_changed,
        STATUS,
        GROWTHCHANNEL as growth_channel,
        COUNTRY,
        CITIES,
        REFERRALCODE as referral_code,
        REFERRALCOUNT as referral_count
    from analytics.core.all_subscribers 
)

select * from subscribers