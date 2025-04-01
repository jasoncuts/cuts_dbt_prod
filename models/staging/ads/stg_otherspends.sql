

SELECT date
    ,wpromote_spend,
    fuel_spend,
    ambassadors_spend,
    influencer_spend,
    podcast_spend,
    newsletter_spend,
    small_ip_spend
FROM {{source('gsheets','otherspends')}}
