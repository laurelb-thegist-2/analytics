with int1_subscribers as (
    select * from {{ref('int1_subscribers')}}
),

fnl_subscribers as (
    select 
        lower(EMAIL) as Email,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        CASE 
            WHEN 
                Growth_Bucket ILIKE '%Organic/Unknown%' 
                or Growth_Bucket ILIKE '%Growth from Socials%' 
                or Growth_Bucket ILIKE '%Website%' 
            THEN 'Organic'
            WHEN 
                Growth_Bucket ILIKE '%Paid Social Media%' 
                or Growth_Bucket ILIKE '%Search%' 
            THEN 'Paid Social'
            WHEN 
                Growth_Bucket ILIKE '%Referral%' 
            THEN 'Referral'
            WHEN 
                Growth_Bucket ILIKE '%LiveIntent%' 
                or Growth_Bucket ILIKE '%Newsletters%'
            THEN 'Newsletters'
            WHEN 
                Growth_Bucket ILIKE '%Affiliate/Influencer%'
            THEN 'Affiliate/Influencer'  
            WHEN 
                Growth_Bucket ILIKE '%CoReg%' 
                or Growth_Bucket ILIKE '%Host & Post%' 
            THEN 'API'
            WHEN 
                Growth_Bucket ILIKE '%Scholarship%' or 
                Growth_Bucket ILIKE '%Dojo%' or Growth_Bucket ILIKE '%Social Stance%' or 
                Growth_Bucket ILIKE '%Student Parent Life%' or 
                Growth_Bucket ILIKE '%Other contests%' 
            THEN 'Contests'
        ELSE 'Organic' END as Growth_Summary,
        coalesce(Growth_Bucket, 'Organic/Unknown') Growth_Bucket,
        coalesce(Growth_Int_Bucket, 'N/A') Growth_Int_Bucket,
        coalesce(Growth_Channel, 'Organic/Unknown') Growth_Channel,
        coalesce(Type_of_Churn, 'N/A') Type_of_Churn,
        coalesce(Incentivization, 'Unincentivized') Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from int1_subscribers
)

select * from fnl_subscribers