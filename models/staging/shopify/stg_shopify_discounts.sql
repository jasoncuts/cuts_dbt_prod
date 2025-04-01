WITH line_discount AS 
(
    SELECT l.id
        ,l.order_id
        ,l.quantity
        ,o.ordered_at
        ,t.type
        ,d.discount_application_index
        ,d.amount as discount_amount
        ,t.title 
        ,t.code 
    FROM {{source('shopify_us','lineitems_discount')}} d
    LEFT JOIN {{source('shopify_us', 'lineitems')}} l
    ON d.order_line_id = l.id
    LEFT JOIN {{source('shopify_us','discounts')}} t
    ON l.order_id = t.order_id AND d.discount_application_index = t.index
    LEFT JOIN {{ref('stg_shopify_orders')}} o 
    ON l.order_id = o.id 
    WHERE o.order_source = 'Sales Order'
)

SELECT *
FROM line_discount