WITH inventory AS 
(
    SELECT item AS item_id
        ,max(coalesce(averagecostmli,0)) AS avg_cost
        ,max(coalesce(lastpurchasepricemli,0)) AS last_purchase_price
        ,sum(coalesce(quantityonhand,0)) AS qty_onhand 
        ,sum(coalesce(quantityavailable,0)) AS qty_avail
        ,sum(coalesce(quantitybackordered,0)) AS qty_backordered
        ,sum(coalesce(quantitycommitted,0)) AS qty_committed
        ,sum(coalesce(quantityintransit,0)) AS qty_in_transit
        ,sum(coalesce(qtyintransitexternal,0)) AS qty_in_transit_ext
        ,sum(coalesce(quantityonorder,0)) AS qty_on_order
        ,max(_fivetran_synced) AS inventory_updated_at
    FROM {{source('netsuite','inventory')}} i
    WHERE _fivetran_deleted IS FALSE 
    AND location = 2 
    GROUP BY 1 
)

SELECT *
FROM inventory