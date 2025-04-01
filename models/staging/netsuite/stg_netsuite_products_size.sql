WITH sizes AS 
(
    SELECT d.id AS size_id 
        ,d.name AS size 
    FROM  {{source('netsuite','sizes')}} d
    WHERE d._fivetran_deleted IS FALSE 
)

SELECT *
FROM sizes 