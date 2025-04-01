
{{ config(materialized='table') }}

WITH lineitems AS 
(
    SELECT *
    FROM {{ref('stg_shopify_lineitems')}}
    {# WHERE gift_card IS FALSE #}
),

orders AS 
(
    SELECT *
    FROM {{ref('stg_shopify_orders')}} 
),

launches AS 
(
    SELECT li.product_shop_name
        ,date(min(os.ordered_at)) AS launch_date
    FROM {{ref('stg_shopify_lineitems')}} li
    LEFT JOIN {{ref('stg_shopify_orders')}} os 
    ON li.order_id = os.id
    WHERE order_source in ('Sales Order', 'Mobile App')
    GROUP BY 1
),

lines AS 
(
    SELECT l.*
        ,CASE WHEN lower(l.properties) LIKE '%kit%' THEN true ELSE false END AS kit_discount
        ,CASE WHEN lower(l.sku) LIKE '%gift%' THEN 'gift'
        ELSE l.gender END AS ns_gender
        ,concat(l.gender, concat('_',lower(replace(l.product_type,' ','_')))) AS product_cat
        ,p.launch_date
        ,date_diff(p.launch_date,o.ordered_at, day) AS days_since_launch
        ,date_diff(p.launch_date,current_date, day) AS days_on_sale
        ,o.order_number
        ,o.customer_name
        ,o.shipping_address
        ,o.shipping_address_line_2
        ,o.shipping_address_province_code
        ,o.shipping_address_country_code
        ,o.ordered_at
        ,o.financial_status
        {# ,o.fulfillment_status #}
        ,o.total_discounts
        ,o.taxes_included
        ,o.total_tax
        ,o.total_shipping
        ,o.total_order_price
        ,o.subtotal_order_price
        ,o.orders_running_count
        ,o.days_since_last_order
        ,o.days_since_first_order
        ,o.customer_id 
        ,o.email
        ,o.order_source
    FROM lineitems l 
    LEFT JOIN orders o 
    ON l.order_id = o.id
    LEFT JOIN launches p 
    ON l.product_shop_name = p.product_shop_name 
    WHERE o.cancelled_at IS NULL 

)

SELECT *
FROM lines 
WHERE order_source = 'Sales Order'