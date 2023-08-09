{#
  create_table(relation, sql, config)

  Parameters
  ----------
  relation: api.Relation
  sql: str
  config: Dict[str, Any]
          Usually we send the whole node config in this input parameter so adapters in general
          can access various informations they may need to build the tables.
#}
{% macro create_table(relation, sql, config) -%}
  {{ adapter.dispatch('create_table', 'dbt_flow')(relation, sql, config) }}
{%- endmacro %}


{% macro default__create_table(relation, sql, config) -%}
  {#/*
    In method `drop_relation` dbt checks if the type of the relation is not None.
    If it is then it raises. We force it to be of type table then.
  */#}
  {%- set relation = relation.incorporate(type='table') -%}
  {%- set sql_header = config.config.get('sql_header', none) -%}

  {%- set final_sql -%}

    {{ sql_header if sql_header is not none }}

    create table {{ relation }}
    as (
      {{ sql }}
    );

  {%- endset -%}

  {#/*
    We need to test first if the table already exists. If it does, then delete it.
    Contrary to the original materialization script from dbt, here we just check for existance
    and not worry about keeping backups or intermediate relations.
  */#}
  {%- set existing_relation = load_cached_relation(relation) -%}

  {%- if existing_relation -%}
    {%- do adapter.drop_relation(existing_relation) -%}
  {%- endif -%}

  {% call statement('main') %} 
    {{ final_sql }}
  {% endcall %}

  {#/*
    After dropping the relation, dbt's cache will be cleared. After creating the table we
    make sure it keeps as added to the cache again so later invocations of the same relation
    will drop the table in case it already exists -- this should not happen in dbt_flow but
    it's implemented here anyways for precaution. It's the correct flow anyways.
  */#}
  {%- do adapter.cache_added(relation) -%}

  {% do adapter.commit() %}

{%- endmacro %}
