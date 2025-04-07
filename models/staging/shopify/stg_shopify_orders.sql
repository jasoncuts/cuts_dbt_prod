WITH order_items AS 
(
    SELECT order_id
        ,sum(quantity) AS item_count
        ,SUM(coalesce(avg_cost,0)) AS items_cost
        ,sum(CASE WHEN gender = 'wom' THEN quantity ELSE 0 END ) AS women_item_count
        ,sum(quantity) - sum(CASE WHEN gender = 'wom' THEN quantity ELSE 0 END ) AS men_item_count
        ,sum(rev_refunded) AS total_refunded
    FROM {{ref('stg_shopify_lineitems')}}
    WHERE gift_card IS FALSE
    GROUP BY 1 
),

orders AS 
(
    SELECT o.id
        ,o.note 
        ,o.order_number
        ,o.number AS no_of_orders
        ,o.test 
        ,o.payment_gateway_names  AS paid_by
        ,'' as processing_method
        ,DATETIME(o.created_at, 'America/Los_Angeles') AS ordered_at
        ,DATETIME(o.updated_at, 'America/Los_Angeles') AS updated_at
        ,DATETIME(TIMESTAMP(o.processed_at), 'America/Los_Angeles') AS processed_at
        ,DATETIME(o.cancelled_at, 'America/Los_Angeles') AS cancelled_at
        ,DATETIME(o.closed_at, 'America/Los_Angeles') AS closed_at
        ,o.cancel_reason
        ,o.financial_status
        ,o.fulfillment_status
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.address2') AS shipping_address
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.address2') AS shipping_address_line_2
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.city') AS shipping_address_city
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.country') AS shipping_address_country
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.country_code') AS shipping_address_country_code
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.province') AS shipping_address_province
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.province_code') AS shipping_address_province_code
        ,JSON_EXTRACT_SCALAR(o.shipping_address, '$.zip') AS shipping_address_zip
        ,JSON_EXTRACT_SCALAR(o.billing_address, '$.name') AS customer_name
        ,oi.total_refunded
        ,oi.item_count
        ,oi.items_cost
        ,oi.women_item_count
        ,oi.men_item_count
        ,CASE WHEN oi.women_item_count = 0 THEN 'men_order'
            WHEN oi.men_item_count = 0 THEN 'wom_order'
            ELSE CASE WHEN oi.men_item_count >= oi.women_item_count THEN 'men_mixed'
                WHEN oi.women_item_count > oi.men_item_count THEN 'wom_mixed'
                ELSE 'men_order'
                END 
            END AS order_gender
        ,o.confirmed AS ordered_confirmed
        ,o.total_weight
        ,o.total_line_items_price
        ,o.total_discounts
        ,o.taxes_included
        ,o.total_tax
        ,o.presentment_currency
        ,CAST(JSON_VALUE(JSON_VALUE(o.total_price_set, '$.presentment_money'), '$.amount') AS FLOAT64) AS presentment_amount
        ,current_total_duties_set  AS total_duties
        ,s.price AS total_shipping
        ,s.discounted_price AS discounted_shipping
        ,s.source AS shipping_source
        ,s.title AS shipping_method
        ,coalesce(o.total_price,0) AS total_order_price
        ,o.subtotal_price AS subtotal_order_price
        ,o.current_total_price AS adjusted_total_order_price
        ,o.current_total_tax AS adjusted_total_tax
        ,o.current_total_discounts AS adjusted_total_discount
        ,o.current_subtotal_price AS adjusted_subtotal_order_price
        ,SAFE_CAST(JSON_EXTRACT_SCALAR(o.customer, '$.id') AS INT64) as customer_id
        ,o.email
        ,o.checkout_token
        ,source_name
        ,CASE WHEN 
            (
                source_name = '158139842561'
            )THEN 'Mobile App'
            WHEN
            (
                source_name = '6167201' OR
                source_name = '426072'
            )THEN 'Hydrogen'
            WHEN  
            (
                 source_name  ='web' OR 
                 source_name ='2827275' OR 
                 source_name ='294517' OR 
                 source_name ='3469081' OR 
                 source_name ='2329312' OR 
                 source_name ='5302775' OR 
                 source_name ='4685273' OR 
                 source_name ='273625' or
                 source_name ='3890849' or
                 source_name = '6737835'
            ) THEN 'Sales Order'
            WHEN  
            (
                 source_name  ='2376822'
            ) THEN 'Grin Influencer Order'
            WHEN 
            (
                source_name ='352363' OR
                source_name ='Returnly Exchanges'
            ) THEN 'Returnly'
            WHEN 
            (
                source_name = '2611619' OR
                source_name = 'Honeycomb'
            )THEN 'Honeycomb Wholesale'
            WHEN 
            (
                source_name = '2612735'
            )THEN 'Route Replacement'
            WHEN 
            (
                source_name = '1424624'
            )THEN 'CS Live Chat Order'
            WHEN 
            (
                source_name = 'shopify_draft_order' OR
                source_name = 'iphone' OR
                source_name = '1830279'
            )THEN 'Draft Orders'
            WHEN 
            (
                source_name = 'exchange'
            )THEN 'Exchange'
            WHEN 
            (
                source_name = 'Cymbio'
            )THEN 'Cymbio Wholesale'
            ELSE 'Undefined' END AS order_source
    FROM {{source('shopify_us','orders')}} o
    LEFT JOIN {{source('shopify_us','shipping')}} s 
    ON o.id = s.order_id
    LEFT JOIN order_items oi 
    ON o.id = oi.order_id
),
order_data AS (
    SELECT *,
        MIN(ordered_at) OVER (PARTITION BY customer_id) AS first_order_date,
        LAG(DATE(ordered_at)) OVER (PARTITION BY customer_id ORDER BY ordered_at ASC) AS previous_order_date,
        RANK() OVER (PARTITION BY customer_id ORDER BY ordered_at) AS orders_running_count,
        SAFE_CAST(1 AS FLOAT64) AS ex_rate
    FROM orders
)

SELECT *,
    DATE_DIFF(DATE(ordered_at), DATE(first_order_date), DAY) AS days_since_first_order,
    DATE_DIFF(DATE(ordered_at), DATE(first_order_date), MONTH) AS months_since_first_order,
    DATE_DIFF(DATE(ordered_at), previous_order_date, DAY) AS days_since_last_order,
    CASE 
        WHEN DATE_DIFF(DATE(ordered_at), DATE(first_order_date), DAY) = 0 
             AND orders_running_count = 1 THEN 'new_order'
        WHEN DATE_DIFF(DATE(ordered_at), DATE(first_order_date), DAY) = 0 
             AND orders_running_count <> 1 THEN 'new_returning_order'
        ELSE 'returning_order' 
    END AS order_type
FROM order_data
