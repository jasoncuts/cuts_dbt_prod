
{{ config(materialized='table') }}

SELECT  
  *
FROM {{ref('int_ga4_sessions_info')}}
