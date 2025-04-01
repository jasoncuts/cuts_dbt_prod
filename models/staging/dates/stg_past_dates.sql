
WITH past AS 
(
    SELECT date(ordered_at) AS date
    FROM {{ref('stg_shopify_orders')}} 
    GROUP BY 1
)

SELECT date
FROM past 
