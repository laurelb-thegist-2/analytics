with special_edition as (
    select * 
    from {{ref('fnl3a_special_edition')}}
),

regular as (
    select * 
    from {{ref('fnl3b_regular')}}
),

all_newsletters as (
    select *
    from special_edition
    union
    select * 
    from regular
)

select * from all_newsletters 
ORDER BY 1,2,4,3