WITH refund AS 
(
    SELECT id
        ,created_at AS refunded_at
        ,order_id AS refunded_order
        ,user_id
        ,note
        ,restock 
    FROM {{source('shopify_us', 'refund')}} r 
)

SELECT *
FROM refund