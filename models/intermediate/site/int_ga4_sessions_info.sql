{{ config(materialized='table') }}

WITH ga_sessions AS (
    SELECT 
        g.*,
        CASE WHEN l.order_id IS NOT NULL THEN TRUE ELSE FALSE END AS converted,
        (o.subtotal_order_price + o.discounted_shipping) AS order_revenue,
        {{ dbt_utils.star(
            from=ref('int_ga4_sessions_duration'),
            except=["session_id"],
            relation_alias='a'
        ) }}
    FROM {{ ref('int_ga4_sessions') }} g
    LEFT JOIN {{ ref('int_ga4_sessions_duration') }} a 
        ON g.session_id = a.session_id
    LEFT JOIN {{ ref('int_ga4_orders_last_touch') }} l 
        ON g.session_id = l.session_id
    LEFT JOIN {{ ref('mart_orders') }} o 
        ON cast(l.order_id as string) = cast(o.id as string)
    WHERE date(g.time) > DATE('2023-08-01')
)

SELECT *
FROM ga_sessions
