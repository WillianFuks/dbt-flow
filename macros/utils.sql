{#/*
  build_flow_identifier(test_name, identifier)

  dbt_flow works by creating the nodes in mocked relations. This macro is responsible
  for creating the name of such relations. They are simply the test name added as suffix
  of the node name.

  Parameters
  ----------
    test_name: str
               Name of the input test, such as "test_customers1".
    identifier: str
                Name of the node itself, such as "orders".

  Returns
  -------
    str: New mocked name for building the table.
*/#}
{% macro build_flow_identifier(test_name, identifier) %}
    {% do return(test_name ~ '__' ~ identifier) %}
{% endmacro %}


{#/*
  get_graph_node_config(attribute_name, attribute_value)

  Scans throught the graph variable and looks for all entries in "nodes", "metrics"
  and "sources" keys looking for one whose node config key `attribute_name` matches
  input `attribute_value`.

  Parameters
  ----------
  attribute_name: str
                  Name of the attribute to query objects from, such as "name" or
                  "unique_id".
  attribute_value: str
                   Value to filter objects from. For instance, if "orders" then
                   query all objects with input `attribute_name` that is equal
                   to "orders".

  Returns
  -------
  Optional[List[Dict[str, Dict[str, Any]]]]
*/#}
{%- macro get_graph_node_config(attribute_name, attribute_value) -%}

  {%- do return(
    graph.nodes.values() | selectattr(attribute_name, '==', attribute_value) | first |
    default(graph.metrics.values() | selectattr(attribute_name, '==', attribute_value) | first, true) |
    default(graph.sources.values() | selectattr(attribute_name, '==', attribute_value) | first, true)) -%}

{%- endmacro -%}

{#/*
  drop_mocked_tables(cached_nodes)

  Parameters
  ----------
    cached_nodes: List[str]
                  Contains all node unique_ids that were processed.
    test_name: str
*/#}
{%- macro drop_mocked_tables(cached_nodes, test_name, verbose=false) -%}

  {%- for node_uid in cached_nodes -%}

    {%- set mocked_relation = cached_nodes[node_uid]['mocked_relation'] -%}

    {%- do dbt_flow.log(test_name, 'Deleting mocked relation "' ~ mocked_relation, verbose) -%}

    {%- do adapter.drop_relation(mocked_relation.incorporate(type='table')) -%}

    {%- do dbt_flow.log(test_name, 'Successfully deleted mocked relation ' ~ mocked_relation, verbose) -%}

  {%- endfor -%}

{%- endmacro -%}


{#/*
  is_incremental()

  This macro is a mock that is used to replace the original `is_incremental()` macro so that we can make the rendering
  return `true` when the incrementality should be triggered. This helps when testing incremental nodes that builds
  their sql query differently if the node is of incremental type.
*/#}
{%- macro is_incremental() -%}
   {%- do return(true) -%}
{%- endmacro -%}


{#/*
  update_test_options(test_setup, global_options)

  Parameters
  ----------
  test_setup: Dict[str, str|Dict[str, Any]]
              Contains overall options and settings for how to run
              the specific test. In this macro we are mainly interested
              in the key "options".

  global_options: Dict[str, Any]
                  Global settings as defined by user-input when calling `run_tests`.
*/#}
{%- macro update_test_options(test_setup, global_options) -%}
  {%- set options = {} -%}
  {%- do options.update(var('dbt_flow_config', {})) -%}
  {%- do options.update(global_options) -%}
  {%- do options.update(test_setup.get('options', {})) -%}
  {%- do test_setup.options.update(options) -%}
{%- endmacro -%}


{%- macro raise_error(error_message) -%}
  {%- do exceptions.raise_compiler_error('\x1b[31m' ~ error_message ~ '\x1b[0m') -%}
{%- endmacro -%}


{%- macro make_temp_relation(relation, suffix='tmp_dbt_flow_') -%}
   {%- do return(api.Relation.create(relation.database, relation.schema, suffix ~ relation.identifier)) -%}
{%- endmacro -%}


{#/*
  process_expected_columns(columns_to_compare)

  At times it may be necessary to apply some function transformations to columns in the
  expected data in order to be able to test and compare the expected with the observed.
  For instance, for BigQuery if the column is of type 'ARRAY' then we need to cast it
  to `TO_SAFE_JSON` in order to be able to compare both columns as `dbt_utils` does not
  offer support for comparing ARRAYs. The solution implemented here is generic enough
  so each adapter may have its nuances and still be contemplated.

  Parameters
  ----------
  columns_to_compare: List[api.Column]
                      This should be the columns obtained from the expected node throught
                      the macro `adapter.get_columns_in_relation()`

  Returns
  -------
  List[str]: quoted columns that should be sent to `dbt_utils.expect()` macro.
*/#}
{%- macro process_expected_columns(columns_to_compare) -%}

  {%- set result = [] -%}

  {%- for col in columns_to_compare -%}
    {%- do result.append(adapter.dispatch('process_expected_column', 'dbt_flow')(col)) -%}
  {%- endfor -%}

  {%- do return(result) -%}

{%- endmacro -%}


{%- macro default__process_expected_column(col) -%}
  {%- do return(col.quoted) -%}
{%- endmacro -%}


{#/*
  ARRAYs and STRUCTs in BigQuery cannot be used for comparisons so they are casted to a STRING
  format.
*/#}
{%- macro bigquery__process_expected_column(col) -%}

  {%- if col.data_type[:5] == 'ARRAY' or col.data_type[:6] == 'STRUCT' -%}
    {%- do return('TO_JSON_STRING(' ~ col.quoted ~ ') AS ' ~ col.name) -%}
  {%- else -%}
    {%- do return(col.quoted) -%}
  {%- endif -%}

{%- endmacro -%}


{%- macro log(test_name, message, flag) -%}
  {{ log('[' ~ test_name ~ ']: ' ~ message, flag) }}
{%- endmacro -%}
