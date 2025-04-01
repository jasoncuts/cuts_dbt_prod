WITH msrp AS 
(   

    SELECT item AS item_id 
            ,{{ dbt_utils.pivot('price_type', dbt_utils.get_column_values(ref('stg_netsuite_products_price_pre'), 'price_type')
            ,agg='max'
            ,then_value='price')}}
    FROM {{ref('stg_netsuite_products_price_pre')}} 
    WHERE currencypage = 1
    AND _fivetran_deleted IS FALSE 
    GROUP BY 1 
)

SELECT *
FROM msrp 