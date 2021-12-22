with user_data_summary as (
    select * from {{ref('fnl_user_data_summary')}}
)

select 
   email,
   Growth_Channel,
   status,
   referral_code,
   referral_count,
   cast(FIRST_SEND as DATE) first_send,
   date_status_changed,
   MOST_RECENT_SEND,
   UNIQUE_OPEN_RATE
from user_data_summary
Where (referral_code is not null and status != 'Active' and date_status_changed > '2021-12-10') and (referral_count = 0 or referral_count is NULL)
ORDER BY first_send DESC 
limit 100000
