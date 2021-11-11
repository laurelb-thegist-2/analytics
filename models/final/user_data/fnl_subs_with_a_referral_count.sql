with SUBSCRIBERS as (
    select * from {{ref('stg_subscribers')}}
)


select 
    EMAIL,
    STATUS,
    country,
    referral_code,
    referral_count
from SUBSCRIBERS
where status = 'Active' and referral_count > 0


