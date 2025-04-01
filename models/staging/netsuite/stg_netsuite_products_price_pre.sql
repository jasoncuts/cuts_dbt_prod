WITH msrp AS 
(   

    SELECT *,
    LOWER(REPLACE(REGEXP_REPLACE(pricelevelname, r'[.,/#!$%^&*;:{}=_`~()-]', ''), ' ', '_')) AS price_type
    FROM {{source('netsuite','msrp')}} 
    WHERE currencypage = 1
    AND _fivetran_deleted IS FALSE 
)

SELECT *
FROM msrp 