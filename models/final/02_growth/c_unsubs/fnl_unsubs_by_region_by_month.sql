with unsubs as (
    select * from {{ref('int1_unsubs')}}
),

regional_monthly_unsubs as (
    select
        date_trunc('month', Unsub_Date) as Month,
        SUM(CASE WHEN City ilike '%TOR%' THEN 1 ELSE 0 END) AS TOR,
        SUM(CASE WHEN City ilike '%OTT%' THEN 1 ELSE 0 END) AS OTT,
        SUM(CASE WHEN Country = 'CA' AND (City ilike '%None%' OR City IS NULL) THEN 1 ELSE 0 END) AS 'CA - None',
        SUM(CASE WHEN City ilike '%BOS%' THEN 1 ELSE 0 END) AS BOS,
        SUM(CASE WHEN City ilike '%CHI%' THEN 1 ELSE 0 END) AS CHI,
        SUM(CASE WHEN City ilike '%DAL%' THEN 1 ELSE 0 END) AS DAL,
        SUM(CASE WHEN City ilike '%DEN%' THEN 1 ELSE 0 END) AS DEN,
        SUM(CASE WHEN City ilike '%DC%' THEN 1 ELSE 0 END) AS DC,
        SUM(CASE WHEN City ilike '%LA%' THEN 1 ELSE 0 END) AS LA,
        SUM(CASE WHEN City ilike '%NYC%' THEN 1 ELSE 0 END) AS NYC,
        SUM(CASE WHEN City ilike '%PHI%' THEN 1 ELSE 0 END) AS PHI,
        SUM(CASE WHEN City ilike '%SEA%' THEN 1 ELSE 0 END) AS SEA,
        SUM(CASE WHEN Country = 'US' AND (City ilike '%None%' OR City IS NULL) THEN 1 ELSE 0 END) AS 'US - None'
    from unsubs
    GROUP BY 1
)

select * from regional_monthly_unsubs
where month > '2021-12-31'
ORDER BY Month desc