{% macro bigquery__create_table(relation, sql, config) -%}

  {%- set relation = relation.incorporate(type='table') -%}
  {%- set raw_partition_by = config.config.get('partition_by', none) -%}
  {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}

  {%- set sql_header = config.config.get('sql_header', none) -%}

  {%- set final_sql -%}

    {{ sql_header if sql_header is not none }}

    create or replace table {{ relation }}
    {{ partition_by(partition_config) }}

    {% if 'partition_expiration_days' in config.config %}
      OPTIONS(
        partition_expiration_days={{ config.config['partition_expiration_days'] }}
      )
    {% endif %}

    as (
      {{ sql }}
    );

  {%- endset -%}

  {%- set existing_relation = load_cached_relation(relation) -%}

  {%- if existing_relation -%}
    {%- do adapter.drop_relation(existing_relation) -%}
  {%- endif -%}

  {% call statement('main') %} 
    {{ final_sql }}
  {% endcall %}

  {%- do adapter.cache_added(relation) -%}

  {% do adapter.commit() %}

{%- endmacro %}
