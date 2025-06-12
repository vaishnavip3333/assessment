{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_id',
        indexes=[
            {'columns': ['customer_id']} 
        ]
    )
}}


{% set sequence_query %}
    CREATE SEQUENCE IF NOT EXISTS customer_id_seq START WITH 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
{% set alter_sequence_query %}
    ALTER SEQUENCE customer_id_seq RESTART WITH 1;
{% endset %}
{% do run_query(alter_sequence_query) %}
{% endif %}

WITH src_data AS (
    SELECT *
    FROM {{ source('jaffle_shop', 'customers') }}
),

intermediate AS (
    SELECT
        nextval('customer_id_seq') AS customer_id,
        id AS source_id,
        SPLIT_PART(name, ' ', 1) AS first_name,
        SPLIT_PART(name, ' ', 2) AS last_name,
        name AS full_name
    FROM src_data
),

final AS (
    SELECT
        intermediate.customer_id,
        intermediate.source_id,
        intermediate.first_name,
        intermediate.last_name,
        intermediate.full_name
    FROM intermediate
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS existing
    ON intermediate.source_id = existing.source_id
        WHERE existing.customer_id IS NULL
        OR LOWER(intermediate.full_name) <> LOWER(existing.full_name)
    {% endif %}
)

SELECT * FROM final