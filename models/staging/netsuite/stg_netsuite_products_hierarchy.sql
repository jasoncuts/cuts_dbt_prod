WITH hierarchy AS 
(
    SELECT m.item AS item_id
        ,m.hierarchynode 
        ,m2.description 
        ,split(m2.description, ':') [safe_offset(0)] AS gender 
        ,split(m2.description, ':') [safe_offset(1)]  AS main_cat
        ,split(m2.description, ':') [safe_offset(2)]  AS cat 
        ,split(m2.description, ':') [safe_offset(3)]  AS sub_cat
        ,m2.name 
    FROM {{source('netsuite','item_merc_hierarchy')}} m 
    LEFT JOIN {{source('netsuite','item_merc_node')}} m2 
    ON m.hierarchynode = m2.id
)

SELECT *
FROM hierarchy