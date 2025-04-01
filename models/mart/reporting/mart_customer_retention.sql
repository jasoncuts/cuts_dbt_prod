WITH x AS (
  SELECT
    customer_id,
    DATE(first_order_date) AS first_order_date,
    DATE(ordered_at) AS order_date
  FROM {{ ref("mart_orders") }}
),

first_orders AS (
  -- Filter to get only customers who placed their first order
  SELECT
    customer_id,
    first_order_date
  FROM x
),

returning_customers AS (
  -- Get all the orders after the first order date (but not on the same day)
  SELECT
    f.customer_id,
    DATE_TRUNC(x.order_date, MONTH) AS return_month
  FROM first_orders f
  JOIN x ON f.customer_id = x.customer_id
  WHERE x.order_date > f.first_order_date
),

new_customers AS (
  -- Get the count of new customers per first order month
  SELECT
    DATE_TRUNC(first_order_date, MONTH) AS first_order_month,
    COUNT(DISTINCT customer_id) AS new_customers
  FROM first_orders
  GROUP BY first_order_month
),

final_cte AS (
  SELECT
    DATE_TRUNC(f.first_order_date, MONTH) AS first_order_month,
    r.return_month,
    COUNT(DISTINCT r.customer_id) AS returning_customers,
    nc.new_customers
  FROM first_orders f
  LEFT JOIN returning_customers r ON f.customer_id = r.customer_id
  LEFT JOIN new_customers nc ON DATE_TRUNC(f.first_order_date, MONTH) = nc.first_order_month
  GROUP BY first_order_month, r.return_month, nc.new_customers
  ORDER BY first_order_month DESC, r.return_month DESC
)

SELECT
  first_order_month,
  return_month,
  CASE 
    WHEN first_order_month = return_month THEN new_customers 
    ELSE returning_customers 
  END AS returning_customers,
  new_customers
FROM final_cte
