{% set categories = dbt_utils.get_column_values(ref('mart_lineitems'), 'forecast_cat') %}
{% set safe_categories = [] %}

{% for category in categories %}
  {% if category is none %}
    {% do safe_categories.append('not_defined_in_net_suite') %}
  {% else %}
    {% set safe_category = category.lower()
                                .replace(' ', '_')
                                .replace('/', '_')
                                .replace('-', '_')
                                .replace('&', 'and')
                                .replace('.', '') %}
    {% do safe_categories.append(safe_category) %}
  {% endif %}
{% endfor %}

{{ config(materialized='table') }}

WITH cats AS (
    SELECT 
        DATE(ordered_at) AS date,
        {% for category in safe_categories %}
        SAFE_DIVIDE(
            SUM(
                CASE 
                    {% if category == 'not_defined_in_net_suite' %}
                        WHEN forecast_cat IS NULL THEN item_total_price_paid
                    {% else %}
                        WHEN LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(forecast_cat, ' ', '_'), '/', '_'), '-', '_'), '&', 'and'), '.', '')) = '{{ category }}' THEN item_total_price_paid
                    {% endif %}
                    ELSE 0
                END
            ),
            SUM(item_total_price_paid)
        ) AS {{ category }}_rev_ratio
        {% if not loop.last %}, {% endif %}
        {% endfor %}
    FROM {{ ref('mart_lineitems') }}
    GROUP BY 1
)

SELECT *
FROM cats
