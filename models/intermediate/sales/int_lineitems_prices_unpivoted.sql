WITH lineitems_prices AS 
(
    {{ dbt_utils.unpivot(
            relation=ref('int_lineitems_prices_selected'),
            cast_to='float64',
            exclude=['id','order_id','quantity','ordered_at'],
            field_name='type',
            value_name='price_amount'
            ) }}
)

SELECT *
FROM lineitems_prices