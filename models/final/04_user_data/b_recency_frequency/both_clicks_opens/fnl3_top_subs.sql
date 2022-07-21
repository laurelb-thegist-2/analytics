with sub_quality as (
    select * from {{ref('int_recency_frequency')}}
),

sub_quality_by_growth_summary as (
    select
        *
    from sub_quality
    where status = 'Active' and (Recency_Frequency = 'Medium' or Recency_Frequency = 'Good')
)

select * from sub_quality_by_growth_summary
order by 1
limit 1000000