WITH transactions AS 
(
SELECT t.id
    ,t.order_id AS related_order_id
    ,t.kind AS transaction_type
    ,r.id as refund_id 
    ,t.amount 
    ,t.currency
    ,DATETIME(t.created_at, 'America/Los_Angeles') AS created_at 
    ,DATETIME(TIMESTAMP(o.ordered_at), 'America/Los_Angeles') AS ordered_at 
    ,t.gateway AS platform
    ,t.source_name 
    ,t.parent_id AS related_transaction_id
    ,t.status
    ,o.ex_rate
    ,o.presentment_currency
    ,t.amount/nullif(o.ex_rate,0) AS usd_amount
    ,SPLIT(o.order_type, '_')[SAFE_OFFSET(0)] AS nc_rc
    ,o.order_gender
    ,CASE WHEN  
        (
             t.source_name  ='web' OR 
             t.source_name ='2827275' OR 
             t.source_name ='294517' OR 
             t.source_name ='3469081' OR 
             t.source_name ='2329312' OR 
             t.source_name ='5302775' OR 
             t.source_name ='4685273' OR 
             t.source_name ='273625' or
             t.source_name ='3890849' or
             t.source_name = '6737835'
        ) THEN 'Sales Order'
        WHEN  
        (
             t.source_name  ='2376822'
        ) THEN 'Grin Influencer Order'
        WHEN 
        (
            t.source_name ='352363' OR
            t.source_name ='Returnly Exchanges'
        ) THEN 'Returnly'
        WHEN 
        (
            t.source_name = '2611619' OR
            t.source_name = 'Honeycomb'
        )THEN 'Honeycomb Wholesale'
        WHEN 
        (
            t.source_name = '2612735'
        )THEN 'Route Replacement'
        WHEN 
        (
            t.source_name = '1424624'
        )THEN 'CS Live Chat Order'
        WHEN 
        (
            t.source_name = 'shopify_draft_order' OR
            t.source_name = 'iphone' OR
            t.source_name = '1830279'
        )THEN 'Draft Orders'
        WHEN 
        (
            t.source_name = 'exchange'
        )THEN 'Exchange'
        WHEN 
        (
            t.source_name = 'Cymbio'
        )THEN 'Cymbio Wholesale'
        ELSE 'Undefined' END AS order_source
FROM {{source('shopify_us','transactions')}} t 
LEFT JOIN {{ref('stg_shopify_orders')}} o 
ON t.order_id = o.id 
LEFT JOIN {{source('shopify_us','refund')}} r
ON t.order_id = r.order_id

)

SELECT *
FROM transactions