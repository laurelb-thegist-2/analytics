with unsubs as (
    select * from {{ref('int1_unsubs')}}
),

regional_monthly_unsubs as (
    select
        date_trunc('month', Unsub_Date) as Month,
        SUM(CASE WHEN City ilike '%TOR%' THEN 1 ELSE 0 END) AS TOR_Unsubs,
        SUM(CASE WHEN City ilike '%OTT%' THEN 1 ELSE 0 END) AS OTT_Unsubs,
        SUM(CASE WHEN Country = 'CA' AND (City ilike '%None%' OR City IS NULL) THEN 1 ELSE 0 END) AS CA_None_Unsubs,
        SUM(CASE WHEN City ilike '%BOS%' THEN 1 ELSE 0 END) AS BOS_Unsubs,
        SUM(CASE WHEN City ilike '%CHI%' THEN 1 ELSE 0 END) AS CHI_Unsubs,
        SUM(CASE WHEN City ilike '%DAL%' THEN 1 ELSE 0 END) AS DAL_Unsubs,
        SUM(CASE WHEN City ilike '%DEN%' THEN 1 ELSE 0 END) AS DEN_Unsubs,
        SUM(CASE WHEN City ilike '%DC%' THEN 1 ELSE 0 END) AS DC_Unsubs,
        SUM(CASE WHEN City ilike '%LA%' THEN 1 ELSE 0 END) AS LA_Unsubs,
        SUM(CASE WHEN City ilike '%NYC%' THEN 1 ELSE 0 END) AS NYC_Unsubs,
        SUM(CASE WHEN City ilike '%PHI%' THEN 1 ELSE 0 END) AS PHI_Unsubs,
        SUM(CASE WHEN City ilike '%SEA%' THEN 1 ELSE 0 END) AS SEA_Unsubs,
        SUM(CASE WHEN Country = 'US' AND (City ilike '%None%' OR City IS NULL) THEN 1 ELSE 0 END) AS US_None_Unsubs
    from unsubs
    GROUP BY 1
)

select * from regional_monthly_unsubs
where month > '2021-12-31'
ORDER BY Month desc