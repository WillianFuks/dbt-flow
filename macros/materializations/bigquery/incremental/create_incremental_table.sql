{%- macro bigquery__create_incremental_table(relation, sql, config) -%}

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
    {%- set incremental_strategy = config.config.get('incremental_strategy') or 'merge' -%}
    {%- set dest_columns = adapter.get_columns_in_relation(existing_relation) -%}
    {%- set incremental_predicates = config.config.get('predicates', none) or config.config.get('incremental_predicates', none) -%}

    {%- set raw_partition_by = config.config.get('partition_by', none) -%}
    {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
    {%- set partitions = config.config.get('partitions', none) -%}

    {%- if incremental_strategy == 'merge' -%}

      {%- set inc_sql = dbt_flow.bq_get_incremental_merge_sql(target_relation, sql, unique_key, dest_columns, incremental_predicates, config.config) -%}

    {%- elif incremental_strategy == 'insert_overwrite' -%}

      {%- set inc_sql = dbt_flow.bq_generate_incremental_insert_overwrite_build_sql(
        temp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, config.config) -%}

    {%- else -%}

      {%- do dbt_flow.raise_error('Incremental strategy must be either "merge" or "insert_overwrite".') -%}

    {%- endif -%}

    {%- do run_query(inc_sql) -%}

  {%- endif -%}

{%- endmacro -%}


{%- macro bq_get_incremental_merge_sql(target, source, unique_key, dest_columns, incremental_predicates, config) -%}

    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
    {%- set merge_update_columns = config.get('merge_update_columns') -%}
    {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
    {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
    {%- set sql_header = config.get('sql_header', none) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
            {% for key in unique_key %}
                {% set this_key_match %}
                    DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
                {% endset %}
                {% do predicates.append(this_key_match) %}
            {% endfor %}
        {% else %}
            {% set unique_key_match %}
                DBT_INTERNAL_SOURCE.{{ unique_key }} = DBT_INTERNAL_DEST.{{ unique_key }}
            {% endset %}
            {% do predicates.append(unique_key_match) %}
        {% endif %}
    {% else %}
        {% do predicates.append('FALSE') %}
    {% endif %}

    {{ sql_header if sql_header is not none }}

    merge into {{ target }} as DBT_INTERNAL_DEST
        using ({{ source }}) as DBT_INTERNAL_SOURCE
        on {{"(" ~ predicates | join(") and (") ~ ")"}}

    {% if unique_key %}
    when matched then update set
        {% for column_name in update_columns -%}
            {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
            {%- if not loop.last %}, {%- endif %}
        {%- endfor %}
    {% endif %}

    when not matched then insert
        ({{ dest_cols_csv }})
    values
        ({{ dest_cols_csv }})

{%- endmacro -%}


{%- macro bq_generate_incremental_insert_overwrite_build_sql(
  temp_relation, target_relation, sql, unique_key, partition_by, partitions, dest_columns, config
) -%}

  {# --code from dbt-bigquery #}
  {% if partition_by is none %}
    {% set missing_partition_msg -%}
      The 'insert_overwrite' strategy requires the `partition_by` config.
    {%- endset %}
    {% do exceptions.raise_compiler_error(missing_partition_msg) %}
  {% endif %}

  {% if partitions is not none and partitions != [] %}

    {% set predicate -%}
        {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in (
            {{ partitions | join (', ') }}
        )
    {%- endset %}

    {%- set source_sql -%}
      ( {{ sql }} )
    {%- endset -%}

    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}

  {%- else -%}

    {% set predicate -%}
      {{ partition_by.render_wrapped(alias='DBT_INTERNAL_DEST') }} in unnest(dbt_partitions_for_replacement)
    {%- endset %}

    {%- set source_sql -%}
    (
      select
      * from {{ temp_relation }}
    )
    {%- endset -%}

    declare dbt_partitions_for_replacement array<{{ partition_by.data_type_for_partition() }}>;

    {%- do dbt_flow.create_table(temp_relation, sql, config) -%}

    set (dbt_partitions_for_replacement) = (
        select as struct
            array_agg(distinct {{ partition_by.render_wrapped() }} IGNORE NULLS)
        from {{ temp_relation }}
    );

  {%- endif -%}

  {%- set predicates = [] if predicates is none else [] + [predicate] -%}

  merge into {{ target_relation }} as DBT_INTERNAL_DEST
      using {{ source_sql }} as DBT_INTERNAL_SOURCE
      on FALSE

  when not matched by source
    {% if predicates %} and {{ predicates | join(' and ') }} {% endif %}
    then delete

  when not matched then insert
    ({{ dest_cols_csv }})
  values
    ({{ dest_cols_csv }})

  drop table if exists {{ temp_relation }}

{%- endmacro -%}
