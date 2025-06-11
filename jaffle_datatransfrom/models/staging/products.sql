SELECT
    sku,
    name AS product_name,
    type AS product_type,
    price,
    description
FROM {{ source('jaffle_shop', 'products') }}
