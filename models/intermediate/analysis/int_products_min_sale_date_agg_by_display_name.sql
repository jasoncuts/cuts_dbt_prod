
{{ config(materialized='table') }}

WITH products AS 
(
    SELECT product_shop_name AS display_name
        ,date(min(ordered_at)) AS launch_date
    FROM {{ref('mart_lineitems')}}
    GROUP BY 1
),


sales AS 
(
    SELECT l.*
    FROM {{ref('mart_lineitems')}} l
    LEFT JOIN products p 
    ON concat(l.netsuite_name,concat(' ',l.color)) = p.display_name
)

SELECT *
FROM sales 
