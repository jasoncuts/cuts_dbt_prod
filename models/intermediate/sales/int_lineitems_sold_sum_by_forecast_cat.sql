
{{ config(materialized='table') }}


WITH lineitems AS 
(
    SELECT date(ordered_at) AS date
        ,lower(replace(coalesce(CASE WHEN lower(sku) LIKE '%SHT%' THEN 'Mens Tees' 
        WHEN netsuite_sku IN 
        (
        'FPBOT_SHOHYPRFIT_BLCK_S',
        'FPBOT_JOGHYPRFIT_BLCK_XL',
        'FPBOT_SHOHYPRFIT_BLCK_M',
        'FPBOT_JOGHYPRFIT_BLCK_S',
        'FPBOT_SHOHYPRFIT_BLCK_L',
        'FPBOT_SHOHYPRFIT_BLCK_XL'
        ) THEN 'Mens Pants' ELSE NULL END, forecast_cat), ' ', '_')) AS product_cat
        ,SUM(quantity) AS items_count
        ,SUM(coalesce(original_price_msrp, item_total_price_paid)) AS item_gross
        ,SUM(least(original_price_msrp,item_total_price_paid)) AS item_net
,1-SUM(least(original_price_msrp,item_total_price_paid))/nullif(SUM(coalesce(original_price_msrp, item_total_price_paid)),0) AS discount_rate    FROM {{ref('mart_lineitems')}} 
    WHERE lower(sku) NOT LIKE '%sub%'
    AND lower(sku) NOT LIKE '%cap%'
    AND lower(sku) NOT LIKE '%box%'
    AND lower(sku) NOT LIKE '%msk%'
    AND lower(sku) NOT LIKE '%vip%'
    AND lower(sku) NOT LIKE '%gif%'
    AND lower(sku) NOT LIKE '%3PK%'
    AND lower(netsuite_sku) NOT LIKE '%ua%'
    GROUP BY 1,2
)

SELECT *
FROM lineitems 
WHERE product_cat IS NOT NULL