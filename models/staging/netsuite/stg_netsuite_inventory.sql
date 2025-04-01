WITH inventory AS 
(
    SELECT item AS item_id
        ,averagecostmli AS avg_cost
        ,lastpurchasepricemli AS last_purchase_price
        ,location AS location_id
        ,quantityonhand 
        ,quantityavailable 
        ,quantitybackordered 
        ,quantitycommitted 
        ,quantityintransit 
        ,qtyintransitexternal
        ,quantityonorder 
        ,reorderpoint 
        ,_fivetran_synced AS inventory_updated_at
    FROM {{source('netsuite','inventory')}} i
    WHERE _fivetran_deleted IS FALSE 
    AND location = 2 
)

SELECT *
FROM inventory