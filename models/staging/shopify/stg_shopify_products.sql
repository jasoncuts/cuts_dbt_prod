WITH shop_skus AS 
(
    SELECT id 
        ,product_id 
        ,sku
        ,price AS shopify_price
        ,compare_at_price
        ,title AS variant_title 
        ,"position" AS variant_position
        ,inventory_policy
        ,inventory_quantity
        ,created_at AS variant_created_at
        ,option1
        ,option2
        ,option3
        ,requires_shipping
        ,RANK() OVER (PARTITION BY sku ORDER BY created_at) AS dup_sku_idx
    FROM {{source('shopify_us','product_variants')}} pv 
),

shop_products AS 
(
    SELECT id AS product_id
        ,title AS product_title
        ,handle AS product_handle
        ,product_type
        ,created_at AS prod_created_at
        ,published_at
        ,published_scope
        ,status
        ,_airbyte_extracted_at AS prod_updated_at
    FROM {{source('shopify_us','products')}} p 
),

images AS 
(
    SELECT product_id
        ,id AS image_id 
        ,position AS image_position
        ,width AS image_width
        ,height AS image_height
        ,src AS image_url
        --,is_default
        ,LAST_VALUE(position) OVER (PARTITION BY product_id order by created_at desc) AS last_pic
        ,created_at AS image_created_at
    FROM {{source('shopify_us','product_images')}} pi2 
    WHERE position = 1 
),

prod_info AS 
(
    SELECT s.*
        ,p.product_title
        ,p.product_handle 
        ,p.product_type
        ,p.prod_created_at
        ,p.published_at
        ,p.published_scope
        ,p.status
        ,CASE WHEN p.published_at IS NOT NULL AND p.status = 'active' THEN TRUE 
        ELSE FALSE END 
        AS live_shopify
        ,p.prod_updated_at
        ,i.image_id
        ,i.last_pic
        ,i.image_position
        ,i.image_url
        ,i.image_width
        ,i.image_height
        --,i.is_default
        ,i.image_created_at
    FROM shop_skus s 
    LEFT JOIN shop_products p 
    ON s.product_id = p.product_id
    LEFT JOIN images i 
    ON s.product_id = i.product_id
)

SELECT *
FROM prod_info