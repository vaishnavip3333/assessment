{{
    config( 
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_id',
        indexes=[{'columns': ['store_id']}],
         pre_hook=[
            "ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_store_id"
        ],
        post_hook=[
            "ALTER TABLE {{ this }} ADD CONSTRAINT pk_store_id PRIMARY KEY (store_id)"
        ]
    )
}}

{% set sequence_query %}
    CREATE SEQUENCE if not exists store_id_seq start with 1;
{%endset%}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
{% set alter_sequence_query %}
    ALTER SEQUENCE store_id_seq RESTART WITH 1;
{% endset %}
{% do run_query(alter_sequence_query) %}
{% endif %}

with src_data as (
    select *
    from {{ source('jaffle_shop','stores')}}
),

intermediate as (
    select
        nextval('store_id_seq') as store_id,
        id as source_id,
        name as store_name,
        opened_at,
        tax_rate
    from src_data
),
final as (
    select
        intermediate.store_id,
        intermediate.source_id,
        intermediate.store_name,
        intermediate.opened_at,
        intermediate.tax_rate
    from intermediate
    {% if is_incremental() %}
    left join {{ this }} as existing
    on intermediate.source_id = existing.source_id
        where existing.store_id is null
        or lower(intermediate.store_name) <> lower(existing.store_name)
    {% endif %}
)
select * from final