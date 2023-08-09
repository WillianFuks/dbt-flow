{#/*
  create_incremental_table(relation, sql, config)

  Parameters
  ----------
  relation: api.Relation
  sql: str
  config: Dict[str, Any]
          Node whole config.
*/#}
{%- macro create_incremental_table(relation, sql, config) -%}
  {{ adapter.dispatch('create_incremental_table', 'dbt_flow')(relation, sql, config) }}
{%- endmacro -%}


{%- macro default__create_incremental_table(relation, sql, config) -%}

  {%- set target_relation = relation.incorporate(type='table') -%}
  {%- set existing_relation = load_cached_relation(target_relation) -%}

  {%- if not existing_relation -%}

    {%- do dbt_flow.create_table(target_relation, sql, config) -%}

  {%- else -%}

    {%- set temp_relation = dbt_flow.make_temp_relation(target_relation) -%}

    {%- set existing_temp_relation = load_cached_relation(temp_relation) -%}

    {%- if existing_temp_relation -%}
      {%- do adapter.drop_relation(temp_relation.incorporate(type='table')) -%}
    {%- endif -%}

    {%- set unique_key = config.config.get('unique_key') -%}
    {%- set incremental_strategy = config.config.get('incremental_strategy') or 'default' -%}
    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set incremental_predicates = config.config.get('predicates', none) or config.config.get('incremental_predicates', none) -%}

    {% set arg_dict = {'target_relation': target_relation, 'temp_relation': temp_relation,
      'unique_key': unique_key, 'dest_columns': dest_columns, 'incremental_predicates': incremental_predicates} %}

    {%- do dbt_flow.create_table(temp_relation, sql, config) -%}
    {%- do adapter.expand_target_column_types(from_relation=temp_relation, to_relation=target_relation) -%}

    {%- if incremental_strategy in ['default', 'append'] -%}

      {%- set inc_sql = adapter.dispatch('get_incremental_default_sql', 'dbt_flow')(arg_dict) -%}

    {%- elif incremental_strategy == 'delete+insert' -%}

      {%- set inc_sql = adapter.dispatch('get_incremental_delete_insert_sql', 'dbt_flow')(arg_dict) -%}

    {%- else -%}

      {%- do dbt_flow.raise_error('Incremental strategy must be either "default", "append" or "delete+insert".') -%}

    {%- endif -%}

    {%- do run_query(inc_sql) -%}

  {%- endif -%}

{%- endmacro -%}


{% macro default__get_incremental_default_sql(arg_dict) %}

  {%- do return(adapter.dispatch('get_incremental_default_sql', 'dbt')(arg_dict)) -%}

{% endmacro %}


{% macro default__get_incremental_delete_insert_sql(arg_dict) %}

  {% do return(get_delete_insert_merge_sql(arg_dict["target_relation"], arg_dict["temp_relation"],
    arg_dict["unique_key"], arg_dict["dest_columns"], arg_dict["incremental_predicates"])) %}

{% endmacro %}
