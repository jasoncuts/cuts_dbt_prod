{% macro ga_utm_logic() -%}


       CASE
           --Direct traffic's UTM
           WHEN utm_medium = '(none)' AND "utm_source" = '(direct)' THEN 'Direct'
  
           --Organic Search's UTM
           WHEN utm_medium IN ('organic','app') AND "utm_source" IN ('google','bing','yahoo','duckduckgo','ecosia.org','chrome','baidu')THEN 'Organic Search'
                  
           --Paid Search's UTM
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%bing%' THEN 'Paid Search'
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%google%' THEN 'Paid Search'
  
  
           --Organic Social's UTM
           WHEN utm_medium IN ('social','Social') THEN 'Organic Social'
           WHEN "utm_source" = 'instagram-womens' THEN 'Organic Social'
  
          
           --Paid social's UTM
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%facebook%' THEN 'Paid Social'
           WHEN utm_medium = '(not set)' AND utm_source LIKE '%facebook%' THEN 'Paid Social'
           WHEN utm_medium = '(not set)' AND "utm_source" LIKE 'shop_app' THEN 'Paid Social'
           WHEN utm_medium = 'paid' AND utm_source LIKE '%facebook%' THEN 'Paid Social'
           WHEN utm_medium = 'paid-social' THEN 'Paid Social'
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%(not set)%' THEN 'Paid Social'
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%snapchat%' THEN 'Paid Social'
           WHEN utm_medium = 'cpc' AND lower(utm_source) LIKE '%tiktok%' THEN 'Paid Social'
           WHEN lower(utm_medium) = 'paid tiktok' AND lower(utm_source) LIKE '%tiktok%' THEN 'Paid Social'
  
           --Email's UTM
           WHEN utm_medium = 'email' THEN 'Email'
           WHEN lower(utm_source) LIKE  '%klaviyo%' THEN 'Email'
          
           --Newsletter's UTM
           WHEN utm_medium = 'newsletter' THEN 'Newsletter'
          
           --Text's UTM
           WHEN utm_medium = 'text' THEN 'Text'
           WHEN utm_medium = 'sms' THEN 'Text'
           WHEN utm_source = 'attentive' THEN 'Text'
          
           --Influencer's UTM
           WHEN utm_medium LIKE '%influencer%' THEN 'Influencer'
           WHEN utm_medium = 'pr' THEN 'Influencer'
          
           --Affliate's UTM
           WHEN utm_medium = 'affiliate' THEN 'Affliate'
          
           --Display Ads' UTM
           WHEN utm_medium = 'display' THEN 'Display'
           WHEN utm_source = 'criteo' THEN 'Display'
          
           --Referral's UTM
           WHEN utm_medium = 'referral' THEN 'Referral'
           WHEN "utm_source" = 'talkable' THEN 'Referral'
          
           --Podcast's UTM
           WHEN utm_medium = 'podcast' THEN 'Podcast'
          
           --Malomo's UTM (when customers visit the site to see tracking updates)
           WHEN utm_source = 'malomo' THEN 'Shipment Tracking'
           WHEN utm_medium LIKE '%malomo%' THEN 'Shipment Tracking'
          
           --Loyalty program
           WHEN lower(utm_source) LIKE '%loyalty%' THEN 'Loyalty'
           WHEN lower(utm_source) LIKE '%yotpo%' THEN 'Loyalty'
          
           --Installments
           WHEN utm_source IN ('sezzle','klarna') THEN 'Installments'
          
           --QR Code
           WHEN utm_medium = 'qr-code' THEN 'QR Code'
          
           --OTT
           WHEN utm_medium IN ('tv','ott') THEN 'OTT'
          
           ELSE
            CASE 
                WHEN utm_medium = 'cpc' OR utm_medium = 'paid' THEN 'Paid Social'
                WHEN utm_source IS NULL AND utm_medium IS NULL THEN 'Direct'
                --Refresher's UTM
                WHEN utm_source = 'facebook' OR utm_source = 'shop_app' THEN 'Paid Social'
                WHEN utm_medium = 'organic' THEN 'Organic Search'
                ELSE 'Undefined'
            END      
           END AS channel
       ,CASE
           --Paid Search's UTM
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%bing%' THEN 'Bing'
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%google%' THEN 'Google'
          
           --Paid social's UTM
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%facebook%' THEN 'Facebook'
           WHEN utm_medium = 'paid' AND utm_source LIKE '%facebook%' THEN 'Facebook'
           WHEN utm_medium = '(not set)' AND utm_source LIKE '%facebook%' THEN 'Facebook'
           WHEN utm_medium = '(not set)' AND "utm_source" LIKE 'shop_app' THEN 'Facebook'
                   WHEN utm_medium = 'cpc' AND utm_source LIKE '%(not set)%' THEN 'Facebook'
           WHEN utm_medium = 'paid-social' THEN 'Facebook'
           WHEN utm_medium = 'cpc' AND utm_source LIKE '%snapchat%' THEN 'Snapchat'
           WHEN utm_medium = 'cpc' AND lower(utm_source) LIKE '%tiktok%' THEN 'Tiktok'
           WHEN lower(utm_medium) = 'paid tiktok' AND lower(utm_source) LIKE '%tiktok%' THEN 'Tiktok'
           ELSE NULL
           END AS paid_platform
{%- endmacro %}

