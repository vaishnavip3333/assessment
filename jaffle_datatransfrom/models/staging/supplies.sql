{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['source_id', 'product_id'],
        indexes=[{'columns': ['source_id', 'product_id']}],
        pre_hook=[
            "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_supply_id"
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_supply_id PRIMARY KEY (supply_id)"
        ]
    )
}}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS supply_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE supply_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT *
    FROM {{ source('jaffle_shop', 'supplies') }}  
),
products AS (
    SELECT * FROM {{ ref('products') }}
),

intermediate AS (
    SELECT
        nextval('supply_id_seq') AS supply_id,
        id AS source_id,
        src_data.name,
        cost,
        perishable,
        products.product_id
    FROM src_data 
    left join products
        on src_data.sku = products.sku
),

final AS (
    SELECT
        intermediate.supply_id,
        intermediate.source_id,
        intermediate.name,
        intermediate.cost,
        intermediate.perishable,
        intermediate.product_id
    FROM intermediate
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.source_id = existing.source_id and
         intermediate.product_id = existing.product_id
           
    WHERE
        existing.source_id IS NULL
        OR LOWER(intermediate.name) <> LOWER(existing.name)
        OR intermediate.cost <> existing.cost
        OR intermediate.perishable IS DISTINCT FROM existing.perishable
        
    {% endif %}
)

SELECT * FROM final