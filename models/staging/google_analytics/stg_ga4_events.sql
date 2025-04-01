WITH all_events AS 
(
    SELECT e.user_pseudo_id AS user_id 
        ,TIMESTAMP_MICROS(event_timestamp) AS time
        ,concat(user_pseudo_id, concat('.',param.value.int_value)) AS session_id
        ,e.event_name AS event_table_name
    FROM {{source('ga4','event')}} e 
    CROSS JOIN UNNEST(event_params) AS param  
    WHERE param.key = 'ga_session_id'
)

SELECT *
FROM all_events 