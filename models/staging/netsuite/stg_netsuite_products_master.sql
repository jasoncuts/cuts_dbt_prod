WITH items AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_item')}}
),

price AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_price')}}  
),

categories AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_hierarchy')}} 
),

inventory AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_inventory')}} 
),

seasons AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_season')}} 
),

divisions AS 
(
    SELECT *
    FROM {{ref('stg_netsuite_products_division')}} 
),

products AS 
(    
    SELECT i.*
        ,p.original_price_msrp
        ,p.current_price_msrp
        ,c.description AS product_hierarchy
        ,c.gender AS product_gender
        ,d.name AS main_cat
        ,l.name AS cat
        ,u.name AS sub_cat
        ,c.name AS product_name
        ,si.name AS size
        ,v.forecast_cat
        ,s.season
        ,n.avg_cost
        ,n.last_purchase_price
        ,n.qty_onhand
        ,n.qty_avail
        ,n.qty_committed
        ,n.qty_in_transit
        ,n.qty_in_transit_ext
        ,n.qty_on_order
        ,n.inventory_updated_at
    FROM items i 
    LEFT JOIN price p
    ON i.id = p.item_id
    LEFT JOIN categories c 
    ON i.id = c.item_id
    LEFT JOIN inventory n 
    ON i.id = n.item_id
    LEFT JOIN seasons s 
    ON i.id = s.item_id
    LEFT JOIN divisions v 
    ON i.id = v.item_id
    LEFT JOIN {{source('netsuite','departments')}} d 
    ON i.custitem_psgss_merc_dept = d.id 
    LEFT JOIN {{source('netsuite','classes')}} l 
    ON i.custitem_psgss_merc_class = l.id 
    LEFT JOIN {{source('netsuite','subclasses')}} u
    ON i.custitem_psgss_merc_subclass  = u.id 
    LEFT JOIN {{source('netsuite','sizes')}} si
    ON i.matrixoptioncustitem_psgss_product_size = cast(si.id as string)

)

SELECT *
FROM products

