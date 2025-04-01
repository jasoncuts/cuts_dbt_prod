WITH employees AS 
(
    SELECT *
    FROM {{ref('stg_shopify_customers')}}
    WHERE lower(email) LIKE '%@cutsclothing.com%'
)

SELECT *
FROM employees 