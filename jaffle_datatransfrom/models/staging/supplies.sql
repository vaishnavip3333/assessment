SELECT id AS supply_id, name AS supply_name, cost, perishable, sku
FROM {{ source('jaffle_shop', 'supplies') }}
