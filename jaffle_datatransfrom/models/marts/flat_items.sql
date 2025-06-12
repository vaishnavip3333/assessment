WITH
items AS (
    SELECT * FROM {{ ref('items') }}
),
orders AS (
    SELECT * FROM {{ ref('orders') }}
),
customers AS (
    SELECT * FROM {{ ref('customers') }}
),
stores AS (
    SELECT * FROM {{ ref('stores') }}
),
supplies AS (
    SELECT * FROM {{ ref('supplies') }}
),
products AS (
    SELECT * FROM {{ ref('products') }}
)

SELECT
    i.item_id,
    i.sku,

    -- Product info
    p.product_name,
    p.product_type,
    p.price,
    p.description,

    -- Order Info
    o.order_id,
    o.ordered_at,

    -- Customer Info
    c.customer_id,
    c.customer_name,

    -- Store Info
    s.store_id,
    s.store_name,
    s.opened_at,
    s.tax_rate,

    -- Supplies Info
    sup.supply_id,
    sup.supply_name,
    sup.cost,
    sup.perishable
FROM items i
JOIN orders o ON i.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN stores s ON o.store_id = s.store_id
LEFT JOIN supplies sup ON i.product_id = sup.product_id
LEFT JOIN products p ON i.product_id = p.product_id
