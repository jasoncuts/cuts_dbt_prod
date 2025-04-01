{{ config(materialized='table') }}

WITH retention_pivot AS (
    SELECT 
        customer_id,
        {{ dbt_utils.pivot(
            'CAST(sale_orders_running_count AS STRING)',  
            dbt_utils.get_column_values(
                table = ref('mart_orders'),
                column = 'sale_orders_running_count',
                where = 'sale_orders_running_count <= 10'
            ),
            agg = 'sum',
            then_value = 'days_since_first_sale'
        ) }}
    FROM {{ ref('mart_orders') }}
    GROUP BY 1
)

SELECT *
FROM retention_pivot
