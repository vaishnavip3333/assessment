{{
    config(
        materialization='incremental',
        incremental_strategy='delete+insert',
        unique_key='source_id',
        indexes=[
            {'columns': ['order_id']}
        ],
        post_hook='ALTER TABLE {{ this }} ADD CONSTRAINT pk_order_id PRIMARY KEY (order_id);'
    )
}}

{% set sequence_query %}
    create sequence if not exists order_id_seq start with 1;
{% endset %}
{% do run_query(sequence_query) %}

{% if not is_incremental() %}
    {% set alter_sequence_query %}
        ALTER SEQUENCE order_id_seq RESTART WITH 1;
    {% endset %}
    {% do run_query(alter_sequence_query) %}
{% endif %}

with src_data as (
    select *
    from {{ source('jaffle_shop', 'orders') }}
),

-- bring in customers (assuming `id` is the primary key in customers)
customers as (
    select customer_id, source_id
    from {{ ref('customers') }}
),

intermediate as (
    select
        nextval('order_id_seq') as order_id,
        o.id as source_id,
        c.customer_id,
        o.store_id,
        o.ordered_at,
        o.subtotal,
        o.tax_paid,
        o.order_total
    from src_data o
    left join customers c
        on o.customer = c.source_id -- assuming `o.customer` holds a customer ID
),

final as (
    select
        i.order_id,
        i.source_id,
        i.customer_id,
        i.store_id,
        i.ordered_at,
        i.subtotal,
        i.tax_paid,
        i.order_total
    from intermediate i
    {% if is_incremental() %}
    left join {{ this }} as existing
        on i.source_id = existing.source_id
    where existing.order_id is null
        or i.order_total <> existing.order_total
        or i.ordered_at <> existing.ordered_at
    {% endif %}
)

select * from final