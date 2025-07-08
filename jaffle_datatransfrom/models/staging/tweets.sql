{{
    config(
        materialization='incremental',
        incremental_strategy='delete+insert',
        unique_key=['source_id'],
        indexes=[{'columns': ['source_id']}],

    )
}}

{% set seqence_query %}
    CREATE SEQUENCE IF NOT EXISTS TWEET_ID_SEQ START WITH 1;
{% endset %}
{% do run_query(seqence_query) %}

{% if not is_incremental() %}
    {% set reset_sequence_query %}
        ALTER SEQUENCE TWEET_ID_SEQ RESTART WITH 1;
    {% endset %}
    {% do run_query(reset_sequence_query) %}
{% endif %}

with src_data as (
    select *
    from {{ source('jaffle_shop', 'tweets') }}
),
intermediate as (
    select
        nextval('TWEET_ID_SEQ') as tweet_id,
        id as source_id,                
        user_id,
        tweeted_at,
        content
)
final as (
    select
        intermediate.tweet_id,
        intermediate.source_id,
        intermediate.user_id,
        intermediate.tweeted_at,
        intermediate.content
    from intermediate
    {% if is_incremental() %}
    left join {{ this }} as existing
        on intermediate.source_id = existing.source_id
    where existing.source_id is null
    {% endif %}
) 


select 
    final.tweet_id,
    final.tweeted_at,
    final.content 
from final