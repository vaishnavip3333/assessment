SELECT id AS item_id, order_id, sku
FROM {{ source('jaffle_shop', 'items') }}
