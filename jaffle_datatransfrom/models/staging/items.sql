{{
    config( 
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_id',
        indexes=[{'columns': ['item_id']}],
       
    )
}}

{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS item_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}
--sequence reset on full refresh
{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE item_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT *
    FROM {{ source('jaffle_shop', 'items') }}  
),
products AS (
    SELECT * FROM {{ ref('products') }}
),

intermediate AS (
    SELECT
        nextval('item_id_seq') AS item_id,     
        id AS source_id,                      
        order_id,
        products.product_id 
    FROM src_data 
    left join products
        on src_data.sku = products.sku 
),

final AS (
    SELECT
        intermediate.item_id,
        intermediate.source_id,
        intermediate.order_id,
        intermediate.product_id
    FROM intermediate
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
        ON intermediate.source_id = existing.source_id
    WHERE
        existing.source_id IS NULL
        OR intermediate.order_id <> existing.order_id
        OR intermediate.product_id <> existing.product_id
    {% endif %}
)

SELECT * FROM final

