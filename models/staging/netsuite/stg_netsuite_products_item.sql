WITH netsuite_item AS 
(
    SELECT *
    FROM {{source('netsuite','products')}}
    WHERE _fivetran_deleted IS FALSE 
),

item AS 
(
    SELECT p.id
        ,p.matrixoptioncustitem_psgss_product_size
        ,u.description AS item_description
        ,u.matrixtype
        ,u.weight
        ,u.weightunit AS weight_unit
        ,p.parent AS parent_item
        ,p.externalid AS external_id
        ,coalesce(p.custitem_cuts_old_sku_name,p.externalid) AS current_sku 
        ,u.displayname AS display_name
        ,u.cost 
        ,u.countryofmanufacture AS country_of_manufaturer
        ,lower(u.custitem_cuts_fabric) AS fabric 
        ,u.custitem_cuts_lifecycle AS lifecycle
        ,u.custitem_cuts_material AS material 
        ,u.custitem_cuts_seasonality AS seasonality 
        ,CASE WHEN u.custitem_psgss_gender = 1 THEN 'men'
            WHEN u.custitem_psgss_gender = 2 THEN 'wom'
            WHEN u.custitem_psgss_gender = 3 THEN 'uni'
            ELSE 'undef' END AS gender           
        ,u.custitem_psgss_product_color_desc AS color 
        ,concat(p.displayname,concat(' ',u.custitem_psgss_product_color_desc)) AS product_detail
        {# ,u.custitem_psgss_product_size_desc AS "size" #}
        ,u.saleunit AS sale_unit
        ,u.custitem_psgss_merc_dept 
        ,u.custitem_psgss_merc_class 
        ,u.custitem_psgss_merc_subclass
    FROM netsuite_item p 
    LEFT JOIN  netsuite_item u
    ON coalesce(p.custitem_cuts_old_sku_name,p.externalid) = u.externalid
)

SELECT *
FROM item