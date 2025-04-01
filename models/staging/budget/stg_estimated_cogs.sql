
{{ config(materialized='table') }}

WITH cogs AS 
(
    SELECT date(date) as date
        ,dcogs
        ,net_income
        ,assumed_expenses
    FROM {{source('gsheet','cogs_estimate')}}
)

SELECT *
FROM cogs 
