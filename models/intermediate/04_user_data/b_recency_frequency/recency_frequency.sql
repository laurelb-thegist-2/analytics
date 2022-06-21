with user_data as (
    select * from {{ref('fnl_user_data')}}
),

rfm_analysis as (
select
    Growth_Channel,
    Growth_Int_Bucket,
    Growth_Bucket,
    Growth_Summary,
    Email,
    Status,
    Country,
    Cities,
    Delivered,
    MOST_RECENT_OPEN,
    MOST_RECENT_CLICK,
    Unique_Opens,
    UNIQUE_OPEN_RATE,
    Total_Clicks,
    CLICK_RATE,
    CASE
    WHEN 
        (
            ( 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9)
            ) 
        AND 
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0)
            )
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9)
            ) 
        AND 
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            )
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0)
            ) 
        AND 
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            )
        ) 
    THEN 'Good'
    WHEN
        (
            (
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9)
            ) 
        AND 
            (
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            )
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0)
            ) 
        AND 
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0)
            )
        )     
    THEN 'Medium'
    WHEN
        (
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.01 and Click_Rate > 0)
            ) 
        AND 
            (
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE = 0)
            )
        ) 
    OR 
        (
            (
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE = 0)
            ) 
        AND 
            (
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -14, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -21, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -7, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -14, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK < dateadd(day, -3, GETDATE()) and MOST_RECENT_CLICK >= dateadd(day, -7, GETDATE())  AND Click_Rate <= 10 AND Click_Rate >= 0.1) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                (MOST_RECENT_CLICK >= dateadd(day, -3, GETDATE()) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            )
        ) 
    OR 
        (
            (
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            ) 
        AND 
            (
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                ((MOST_RECENT_OPEN < dateadd(day, -21, GETDATE()) or MOST_RECENT_OPEN is NULL) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE = 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE = 0)
            )
        ) 
    OR 
        (
            (
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE < 0.9 AND UNIQUE_OPEN_RATE >= 0.6) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -14, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -21, GETDATE()) AND UNIQUE_OPEN_RATE >= 0.9) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -7, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -14, GETDATE()) AND UNIQUE_OPEN_RATE < 0.6 and UNIQUE_OPEN_RATE >= 0.3) 
                OR 
                (MOST_RECENT_OPEN < dateadd(day, -3, GETDATE()) and MOST_RECENT_OPEN >= dateadd(day, -7, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0) 
                OR 
                (MOST_RECENT_OPEN >= dateadd(day, -3, GETDATE()) AND UNIQUE_OPEN_RATE < 0.3 and UNIQUE_OPEN_RATE > 0)
            ) 
        AND 
            (
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate = 0 or Click_Rate > 10) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.01 and Click_Rate > 0) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.05 and Click_Rate >= 0.01) 
                OR
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate < 0.1 AND Click_Rate >= 0.05) 
                OR 
                ((MOST_RECENT_CLICK < dateadd(day, -21, GETDATE()) or MOST_RECENT_CLICK is NULL) AND Click_Rate <= 10 AND Click_Rate >= 0.1)
            )
        )
    THEN 'Bad'
    ELSE 'Bad' END as Recency_Frequency
from user_data
)

select * from rfm_analysis

