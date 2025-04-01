WITH seasons AS 
(
    SELECT i.id AS item_id
        ,s.name AS season
    FROM {{source('netsuite','products')}} i
    LEFT JOIN  {{source('netsuite','seasons')}} s
    ON i.custitem_psgss_season = s.id
    WHERE i._fivetran_deleted IS FALSE 
)

SELECT *
FROM seasons 