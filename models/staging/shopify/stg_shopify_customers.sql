WITH customers AS 
(
    SELECT id
        ,first_name
        ,last_name        
        ,email
        ,verified_email
        ,state
        ,accepts_marketing AS accepts_email_marketing
        ,phone
        ,marketing_opt_in_level  AS sms_marketing_level
        ,created_at
        ,updated_at
        ,orders_count
        --,lifetime_duration
        ,total_spent
        ,tax_exempt
        FROM {{source('shopify_us','customers')}}
)

SELECT *
FROM customers 