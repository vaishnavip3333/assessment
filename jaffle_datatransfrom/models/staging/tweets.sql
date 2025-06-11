SELECT id AS tweet_id, user_id AS customer_id, tweeted_at, content
FROM {{ source('jaffle_shop', 'tweets') }}
