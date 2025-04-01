
WITH dates AS 
(
    SELECT *
    FROM {{ref('stg_past_dates')}}
    UNION ALL 
    SELECT *
    FROM {{ref('stg_future_365_days')}}
)

SELECT date
FROM dates 
