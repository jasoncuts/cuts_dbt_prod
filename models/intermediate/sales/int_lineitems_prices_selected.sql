WITH lineitems AS 
(
    SELECT id  
        ,order_id
        ,ordered_at
        ,quantity
        ,item_total_price_paid
        ,greatest(coalesce(original_price_msrp,item_unit_price) ,item_unit_price) AS item_msrp_price
        ,item_unit_price
        ,(greatest(coalesce(original_price_msrp,item_unit_price) ,item_unit_price) - item_unit_price) AS markdown_discount
    FROM {{ref('mart_lineitems')}}
)

SELECT *
FROM lineitems 