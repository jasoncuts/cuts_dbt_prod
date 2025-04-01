WITH orders AS
(
    SELECT TIMESTAMP_MICROS(event_timestamp) AS time
        ,transaction_id_param.value.string_value AS order_id
        ,concat(user_pseudo_id, concat('.',param.value.int_value)) AS session_id
        ,e.user_pseudo_id AS user_id
        ,row_number() OVER (PARTITION BY transaction_id_param.value.string_value ORDER BY TIMESTAMP_MICROS(e.event_timestamp) ASC) AS rn
    FROM {{source('ga4','event')}} e 
    CROSS JOIN UNNEST(event_params) AS param 
    CROSS JOIN UNNEST(event_params) as transaction_id_param 
    WHERE event_name = 'purchase' and param.key = 'ga_session_id' and transaction_id_param.key = 'transaction_id'
)

SELECT *
FROM orders
WHERE rn = 1 