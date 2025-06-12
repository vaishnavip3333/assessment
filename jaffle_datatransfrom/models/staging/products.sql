{{ 
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='sku',
        indexes=[{'columns': ['sku']}],
        pre_hook=[
            "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_product_id"
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_product_id PRIMARY KEY (product_id)"
        ]
    )
}}


{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS product_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
{% set reset_sequence_query %}
    ALTER SEQUENCE product_id_seq RESTART WITH 1;
{% endset %}
{% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT *
    FROM {{ source('jaffle_shop', 'products') }}  
),

intermediate AS (
    SELECT
        nextval('product_id_seq') AS product_id,
        sku,
        name,
        type,
        price,
        description
    FROM src_data
),

final AS (
    SELECT
        intermediate.product_id,
        intermediate.sku,
        intermediate.name,
        intermediate.type,
        intermediate.price,
        intermediate.description
    FROM intermediate
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.sku = existing.sku
    WHERE
        existing.sku IS NULL
        OR LOWER(intermediate.name) <> LOWER(existing.name)
        OR LOWER(intermediate.description) <> LOWER(existing.description)
        OR intermediate.price <> existing.price
        OR intermediate.type <> existing.type
    {% endif %}
)

SELECT * FROM final
