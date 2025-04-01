{{ config(materialized='table') }}

SELECT *
FROM {{source('gsheet','influencer_spend_breakdown')}}