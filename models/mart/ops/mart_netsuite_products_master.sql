{{ config(materialized='table') }}

WITH ns_products AS 
(    
    SELECT *
    FROM {{ref('stg_netsuite_products_master')}}
),

shop_products AS 
(
    SELECT *
    FROM {{ref('stg_shopify_products')}}
),

products_info AS 
(
    SELECT m.*
        ,p.product_id 
        ,p.sku AS shopify_sku
        ,p.shopify_price
        ,p.compare_at_price AS shop_comp_price
        ,p.product_handle
        ,p.live_shopify
        ,p.published_at
        ,p.published_scope
        ,p.status
        ,concat(split(p.image_url,'?')[safe_offset(0)], '?width=300&height=300') AS image_url
        ,p.image_created_at
        ,p.inventory_policy
        ,p.inventory_quantity AS shopify_inventory
        ,p.variant_title
    FROM ns_products m
    LEFT JOIN 
    (
        SELECT *
        FROM shop_products 
        WHERE sku <> ''
        AND lower(sku) NOT like '%cap%'
        AND lower(sku) NOT like '%mem%'
        AND lower(sku) NOT like '%sub%'
        AND lower(sku) NOT like '%protect%'
        AND shopify_price <> 0
    ) p 
    ON m.external_id = p.sku
    WHERE p.dup_sku_idx = 1 OR p.dup_sku_idx IS NULL 
)

SELECT *
    ,last_value(image_url ignore nulls) over (PARTITION BY product_detail order by product_id) AS image_subbed
FROM products_info