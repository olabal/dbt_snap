{% macro default__build_snapshot_table(strategy, sql) %}
{#- customized snapshot table with additional metadata columns #}

    select *, 
        current_timestamp() as ETL_GMTS,
            '{{var('ctrm_odate') }}'::DATE as VALID_FROM_TS,
            '{{ var("valid_to_ts") }}'::TIMESTAMP as VALID_TO_TS,
            '{{ var("current_record_ind") }}'::string as current_record_ind,
         {{ strategy.updated_at }} as INSERT_TS,
        cast(NULL as TIMESTAMP) as UPDATE_TS,
        cast(NULL as TIMESTAMP) as DELETE_TS,
        {#- dbt standard snapshot fields #}
        {{ strategy.scd_id }} as dbt_scd_id,
        {{ strategy.updated_at }} as dbt_updated_at,
        {{ strategy.updated_at }} as dbt_valid_from, 
        nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to
    from (
        {{ sql }}
    ) sbq

{% endmacro %}