WITH line_refund AS 
(
    SELECT l.*
        ,r.created_at AS refund_created_at
        ,r.order_id 
    FROM {{source('shopify_us','lineitems_refund')}} l
    LEFT JOIN {{source('shopify_us', 'refund')}} r 
    ON l.refund_id = r.id 
),

line_refund_at AS
(
    SELECT order_line_id
        ,sum(quantity) AS qty_refunded
        ,sum(subtotal) AS rev_refunded
        ,sum(total_tax) AS tax_refunded
        ,min(refund_created_at) AS refund_created_at
    FROM line_refund
    GROUP BY 1
),

line_discount AS 
(
    SELECT *
    FROM {{ref('stg_shopify_lineitems_discount')}}
),

lineitems AS 
(
    SELECT l.id
        ,l.order_id
        ,l.product_id
        ,p.id AS netsuite_item_id
        ,l.name AS product_name
        ,l.variant_id
        ,l.sku
        ,l.gift_card
        ,l.title
        ,l.variant_title
        ,l.quantity
        ,l.fulfillable_quantity
        ,l.grams AS item_weight
        ,l.taxable
        ,l.pre_tax_price AS item_total_price_paid
        ,l.total_discount AS item_total_discount
        ,{{dbt_utils.star(from=ref('stg_shopify_lineitems_discount'), except=["order_line_id",'order_id'])}}
        ,CASE WHEN dis_1_amt <> 0 THEN t1.type ELSE NULL END AS dis_1_type
        ,CASE WHEN dis_1_amt <> 0 THEN coalesce(t1.title, t1.code) ELSE NULL END AS dis_1_name
        ,CASE WHEN dis_2_amt <> 0 THEN t2.type ELSE NULL END AS dis_2_type
        ,CASE WHEN dis_2_amt <> 0 THEN coalesce(t2.title, t2.code) ELSE NULL END AS dis_2_name
        ,CASE WHEN dis_3_amt <> 0 THEN t3.type ELSE NULL END AS dis_3_type
        ,CASE WHEN dis_3_amt <> 0 THEN coalesce(t3.title, t3.code) ELSE NULL END AS dis_3_name
        ,l.properties
        ,l.price AS item_unit_price
        ,pr.handle AS product_handle
        ,p.display_name AS netsuite_name 
        ,p.color 
        ,p.size 
        ,concat(p.display_name,concat(' ',p.color)) AS product_shop_name
        ,concat(product_name,concat(' ',p.size)) AS product_shop
        ,p.gender
        ,p.season
        ,p.seasonality
        ,p.material
        ,p.lifecycle
        ,p.current_sku
        ,p.external_id AS netsuite_sku
        ,p.cost 
        ,CAST(p.avg_cost AS FLOAT64) as avg_cost
        ,p.last_purchase_price
        ,p.original_price_msrp
        ,CASE WHEN lower(l.name) LIKE '%mystery%' THEN 216 
        WHEN (CAST(p.original_price_msrp AS FLOAT64) <> CAST(0 AS FLOAT64) OR CAST(p.original_price_msrp AS FLOAT64) IS NOT NULL) AND CAST(p.original_price_msrp AS FLOAT64) >= l.price THEN CAST(p.original_price_msrp AS FLOAT64)
        ELSE l.price
        END AS full_price
        ,p.current_price_msrp
        ,p.product_gender
        ,p.fabric
        ,p.main_cat
        ,p.cat
        ,p.sub_cat
        ,CASE WHEN lower(p.forecast_cat) LIKE '%mens%' THEN forecast_cat ELSE 'Others' END AS forecast_cat
        ,coalesce( CASE WHEN lower(sku) LIKE '%sub%' 
            OR lower(sku) LIKE '%vip%'
            OR lower(sku) LIKE '%mem%' 
            THEN 'Membership'
            WHEN lower(sku) LIKE '%cap%'
            OR lower(sku) LIKE '%3pk%'
            OR lower(sku) LIKE '%pck%'
            THEN 'Bundles' 
            WHEN lower(sku) LIKE '%box%'
            OR lower(sku) LIKE '%gif%'
            THEN 'Gifts'
            WHEN lower(sku) LIKE '%msk%'
            OR lower(sku) LIKE '%UA10%'
            THEN 'Masks'
            WHEN lower(sku) LIKE 'acc%'
            THEN 'Accessories'
            WHEN lower(sku) LIKE '%box%'
            THEN 'Boxes'
            ELSE NULL END, p.forecast_cat) AS detailed_forecast_cat
        ,p.product_name AS product_type
        ,l.requires_shipping
        ,l.fulfillment_service
        ,l.fulfillment_status
        ,r.qty_refunded
        ,r.rev_refunded
        ,r.tax_refunded
        ,r.refund_created_at
    FROM {{source('shopify_us', 'lineitems')}} l
    LEFT JOIN line_refund_at r
    ON l.id = r.order_line_id
    LEFT JOIN {{source('shopify_us','products')}} pr 
    ON l.product_id = pr.id
    LEFT JOIN {{ref('stg_netsuite_products_master')}} p
    ON lower(trim(l.sku,' ')) = lower(p.external_id)
    LEFT JOIN line_discount d 
    ON l.id = d.order_line_id
    LEFT JOIN {{source('shopify_us','discounts')}} t1
    ON l.order_id = t1.order_id AND d.dis_1_idx = t1.index
    LEFT JOIN {{source('shopify_us','discounts')}} t2
    ON l.order_id = t2.order_id AND d.dis_2_idx = t2.index
    LEFT JOIN {{source('shopify_us','discounts')}} t3
    ON l.order_id = t3.order_id AND d.dis_3_idx = t3.index
    WHERE lower(name) NOT LIKE '%route%'
    AND l.title <> 'Carbon Neutral Order'
    AND lower(name) NOT LIKE '%shipping%'
)

SELECT *
FROM lineitems
