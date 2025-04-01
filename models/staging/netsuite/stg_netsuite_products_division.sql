WITH divisions AS 
(
    SELECT i.id AS item_id
        ,d.name AS forecast_cat
    FROM {{source('netsuite','products')}} i
    LEFT JOIN  {{source('netsuite','item_merc_division')}} d
    ON i.custitem_psgss_merc_division = d.id
    WHERE i._fivetran_deleted IS FALSE 
)

SELECT *
FROM divisions 