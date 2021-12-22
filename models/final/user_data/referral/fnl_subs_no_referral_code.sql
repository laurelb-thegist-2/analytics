with user_data_summary as (
    select * from {{ref('fnl_user_data_summary')}}
)

select 
   email,
   Growth_Channel,
   status,
   referral_code,
   cast(FIRST_SEND as DATE) first_send,
   MOST_RECENT_SEND,
   UNIQUE_OPEN_RATE
from user_data_summary
Where referral_code is null and status = 'Active' and first_send < '2021-12-08' and UNIQUE_OPEN_RATE > 0.05
ORDER BY first_send DESC 
limit 50000
