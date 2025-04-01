WITH futures AS 
(
    SELECT DATE(ordered_at) AS date
    FROM {{ ref('stg_shopify_orders') }} 
    WHERE DATE(ordered_at) > DATE_SUB(CURRENT_DATE(), INTERVAL 1 YEAR)
    GROUP BY 1
)

SELECT DATE(DATE_ADD(date, INTERVAL 1 YEAR)) AS date
FROM futures
