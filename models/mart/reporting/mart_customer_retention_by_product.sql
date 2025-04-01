WITH x AS (
  SELECT
    mo.customer_id,
    DATE_TRUNC(mo.first_order_date, MONTH) AS first_order_month,
    mo.first_order_date,
    mo.ordered_at,
    mli.product_id,
    mli.product_name,
    pcm.value AS product_category
  FROM {{ ref("mart_orders") }} AS mo 
  JOIN {{ ref("mart_lineitems") }} AS mli
    ON mo.id = mli.order_id
  LEFT JOIN {{ source('gsheet', 'product_category_mapping') }} AS pcm
    ON pcm.product_id = mli.product_id
  WHERE mo.first_order_date = mli.ordered_at
),

first_orders AS (
  -- Identify each customer's first order month and product category
  SELECT
    customer_id,
    first_order_month,
    product_category
  FROM x
  GROUP BY customer_id, first_order_month, product_category
),

returning_customers AS (
  -- Identify returning customers after their first order, regardless of product in the return orders
  SELECT
    f.customer_id,
    f.product_category,
    DATE_TRUNC(mo.ordered_at, MONTH) AS return_month
  FROM first_orders f
  JOIN {{ ref("mart_orders") }} mo
    ON f.customer_id = mo.customer_id
  WHERE mo.ordered_at > f.first_order_month
),

new_customers AS (
  -- Count the number of new customers for each first order month and product category
  SELECT
    first_order_month,
    product_category,
    COUNT(DISTINCT customer_id) AS new_customers
  FROM first_orders
  GROUP BY first_order_month, product_category
)

-- Perform the retention analysis, including new and returning customers in subsequent months
SELECT
  f.first_order_month,
  f.product_category,
  r.return_month,
  COUNT(DISTINCT r.customer_id) AS returning_customers,
  nc.new_customers
FROM first_orders f
LEFT JOIN returning_customers r
  ON f.customer_id = r.customer_id
  AND f.product_category = r.product_category
LEFT JOIN new_customers nc
  ON f.first_order_month = nc.first_order_month
  AND f.product_category = nc.product_category
GROUP BY f.first_order_month, f.product_category, r.return_month, nc.new_customers
ORDER BY f.first_order_month DESC, f.product_category, r.return_month DESC
