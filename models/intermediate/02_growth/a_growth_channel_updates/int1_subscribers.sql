with subscribers as (
    select * from {{ref('stg_subscribers')}}
),

growth_bucket as (
    select 
        EMAIL,
        LEADID,
        list_ID,
        date_status_changed,
        STATUS,
        CASE 
            WHEN 
                Growth_Channel ILIKE 'Organic/Unknown' 
            THEN 'Organic/Unknown'
            WHEN 
                Growth_Channel ILIKE '%Dojo%' 
            THEN 'Dojo'
            WHEN 
                Growth_Channel ILIKE '%CoReg%' 
                and Growth_Channel not ilike '%mowMedia%' 
                and Growth_Channel not ilike '%testMow%' 
                and Growth_Channel not ilike '%NexxtHP%' 
                and Growth_Channel not ilike '%CoReg-Campaign-Source%' 
            THEN 'CoReg'
            WHEN 
                Growth_Channel ILIKE '%Ambassador%' 
            THEN 'Affiliate/Influencer'
            WHEN 
                (Growth_Channel ILIKE '%unpaid%' 
                and Growth_Channel ILIKE '%socialmedia%' 
                and Growth_Channel not ilike '%newsletter%') 
                or Growth_Channel ilike '%citylead%' 
                or Growth_Channel ilike '%instagram%' 
            THEN 'Growth from Socials'
            WHEN 
                Growth_Channel ilike '%mowMedia%'
                or Growth_Channel ilike '%testMow%' 
                or Growth_Channel ilike '%NexxtHP%' 
                or Growth_Channel ilike '%CoReg-Campaign-Source%' 
            THEN 'Host & Post'
            WHEN 
                Growth_Channel ilike '%newsletter%' 
                or Growth_Channel ilike '%paved-paid%' 
            THEN 'Newsletters'
            WHEN 
                Growth_Channel not ilike '%dojo%' 
                and Growth_Channel not ilike '%PLN%' 
                and Growth_Channel not ilike '%socialstance%' 
                and Growth_Channel not ilike '%Social Stance%' 
                and Growth_Channel ilike '%contest%' 
            THEN 'Other Contests'
            WHEN 
                Growth_Channel ilike '%youtube%' 
            THEN 'Paid Social Media - Youtube'
            WHEN 
                (Growth_Channel ilike '%socialmedia%' 
                and Growth_Channel ilike '%paid%' 
                and Growth_Channel not ilike '%contest%' 
                and Growth_Channel not ilike '%unpaid%' 
                and Growth_Channel not ilike '%TikTok%' 
                and Growth_Channel not ilike '%snap%' 
                and Growth_Channel not ilike '%youtube%' 
                and Growth_Channel not ilike '%website%') 
                or Growth_Channel ilike '%fb+%' 
            THEN 'Paid Social Media - FB'
            WHEN 
                Growth_Channel ilike '%socialmedia%' 
                and Growth_Channel ilike '%paid%' 
                and Growth_Channel ilike '%TikTok%' 
            THEN 'Paid Social Media - TikTok'
            WHEN 
                Growth_Channel ilike '%socialmedia%' 
                and Growth_Channel ilike '%paid%' 
                and Growth_Channel ilike '%Snap%' 
            THEN 'Paid Social Media - Snap'
            WHEN 
                Growth_Channel ilike '%Referral%'
            THEN 'Referral'
            WHEN 
                Growth_Channel ilike '%Scholarship%' 
            THEN 'Scholarship'
            WHEN 
                Growth_Channel ilike '%search%'
            THEN 'Search'
            WHEN 
                Growth_Channel ilike '%socialstance%' 
                or Growth_Channel ilike '%Social Stance%' 
            THEN 'Social Stance'
            WHEN 
                Growth_Channel ilike '%pln%' 
                and Growth_Channel ilike '%luckiest%' 
            THEN 'Student Parent Life'
            WHEN 
                Growth_Channel ilike '%website%' 
                and Growth_Channel not ilike '%newsletter%' 
            THEN 'Website'
            WHEN 
                Growth_Channel ilike '%LiveIntent%' 
            THEN 'LiveIntent'
        ELSE 'Organic/Unknown' END as Growth_Bucket,
        CASE 
            WHEN Growth_Channel ILIKE '%digitalviking-DVM%' THEN 'DVM'
            WHEN Growth_Channel ILIKE '%advisio%' or Growth_Channel ilike '%KUBTG03%' THEN 'Advisio/Kubient'
            WHEN Growth_Channel ILIKE '%dmipartners%' THEN 'DMI'
            WHEN Growth_Channel ILIKE '%CoReg-DMS%' THEN 'DMS'
            WHEN Growth_Channel ILIKE '%leadpulse%' THEN 'Leadpulse'
            WHEN Growth_Channel ILIKE '%NexxtHP%' THEN 'NexxtHP'
            WHEN Growth_Channel ILIKE '%LENHP%' or Growth_Channel ilike '%lensa%' THEN 'LENHP'
        ELSE 'N/A' END as Growth_Int_Bucket,
        coalesce(Growth_Channel, 'Organic/Unknown') Growth_Channel,
        CASE
            WHEN status ILIKE '%Unsubscribed%' THEN 'Voluntary'
            WHEN status ILIKE '%Deleted%' THEN 'Non-voluntary'
            WHEN status ILIKE '%Bounced%' THEN 'Bounced'
        ELSE 'N/A' END as Type_of_Churn,
        coalesce(Incentivization, 'Unincentivized') Incentivization,
        Country,
        CITIES,
        referral_code,
        referral_count,
        campaign_name,
        source_brand,
        Partner_Engagement_Surveys
    from subscribers
)

select * from growth_bucket