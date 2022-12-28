{% macro test(model_name, test_name, test_description, options={}) %}
  {{ dbt_flow.ref_tested_model(model_name) }}

  {% if execute %}
    {% if not adapter.check_schema_exists(database=target.database, schema=target.schema) %}
        {% do adapter.create_schema(api.Relation.create(database=target.database, schema=target.schema)) %}
    {% endif %}

    {% set test_configuration = {
      "model_name": model_name,
      "test_name": test_name,
      "description": test_description,
      "options": dbt_flow.merge_configs([options])}
    %}

    {% set mocks_and_expectations_json_str = caller() %}
    {{ dbt_flow.verbose("CONFIG: " ~ test_configuration) }}

    {% do test_configuration.update(dbt_flow.build_mocks_and_expectations(test_configuration, mocks_and_expectations_json_str)) %}
    {% do dbt_flow.run_test(test_configuration) %}

    {# this query is mandatory as otherwise dbt's testing framework will break when trying to execute the test
    against the database #}
    select 1 from (select 1) as t where False
  {% endif %}
{% endmacro %}

{% macro build_mocks_and_expectations(test_configuration, mocks_and_expectations_json_str) %}
  {% set mocks_and_expectations = dbt_flow.split_json_str(mocks_and_expectations_json_str) %}

  {% for mock_or_expectation in mocks_and_expectations %}
    {% do mock_or_expectation.update( {"options": dbt_flow.merge_configs([test_configuration.options, mock_or_expectation.options])}) %}
    {% set input_values = dbt_flow.build_input_values_sql(mock_or_expectation.input_values, mock_or_expectation.options) %}
    {% do mock_or_expectation.update({"input_values": input_values}) %}
  {% endfor %}

  {% set mocks = mocks_and_expectations | selectattr("type", "==", "mock") | list %}
  {% set expectations = mocks_and_expectations | selectattr("type", "==", "expectations") | first %}

  {% for mock in mocks %}
    {% do mock.update({"unique_id": dbt_flow.graph_node(mock.source_name, mock.name).unique_id}) %}
    {% if mock.options.include_missing_columns %}
      {% do dbt_flow.enrich_mock_sql_with_missing_columns(mock, test_configuration.options) %}
    {% endif %}
  {% endfor %}

  {% set mocks_and_expectations_json = {
    "mocks": mocks,
    "expectations": expectations
    } %}

  {{ return (mocks_and_expectations_json) }}
{% endmacro %}

{% macro run_test(test_configuration) %}
  {% set expectations = test_configuration.expectations %}
  {% set node = dbt_flow.model_node(test_configuration.model_name) %}
  {% set node_rel = dbt_flow.build_graph_views(node, test_configuration) %}
  {% set expect_rel = dbt_flow.build_expectations_view(expectations, node, test_configuration) %}

  {% set results = run_query(dbt_utils.test_equality(expect_rel, node_rel)) %}
  {% if results.columns[0].values() %}
    {{log('\n' * 2 ~ 'Expected results are not equal to the actual ones:\n', True)}}
    {{log('*' * 90, True)}}
    {% do results.print_table(max_columns=results.columns.keys() | length) %}
    {{log('\n' ~ '*' * 90 ~ '\n', True)}}
    {% do exceptions.raise_compiler_error('\n\n' ~ 'Flow test for model ' ~ test_configuration.model_name ~ ' failed!!!\n') %}
  {% endif %}
{% endmacro %}

{% macro build_graph_views(node, config) %}
  {{log('input node: ' ~ node.unique_id, True)}}
  {{log('node: ' ~ node | pprint, True)}}
  {{log('\n\n', True)}}

  {% set relation = api.Relation.create(
      database=node.database,
      schema=node.schema, 
      identifier=dbt_flow.build_flow_identifier(config.test_name, node.name)
  ) %}
  
  {% set mock = config.mocks | selectattr("unique_id", "==", node.unique_id) | first %}
  {% if mock %}
    {% do run_query(create_view_as(relation, mock.input_values)) %}
  {% else %}
    {% set ns = namespace(node_query = render(node.raw_code)) %}
    {% for node_id in node.depends_on.nodes %}
      {% set child_node = dbt_flow.node_by_id(node_id) %}
      {{log('this is child node: ' ~ child_node | pprint, True) }}
      {{log('\n\n', True)}}

      {% set child_relation = api.Relation.create(
        database=child_node.database,
        schema=child_node.schema,
        identifier=child_node.name
      ) %}
      {% set mocked_child_relation = api.Relation.create(
        database=child_node.database,
        schema=child_node.schema,
        identifier=dbt_flow.build_flow_identifier(config.test_name, child_node.name)
      ) %}

      {{ log('this is current child_node_id: ' ~ child_node.unique_id, True) }}
      {{ log('this is child_relation: ' ~ child_relation, True) }}
      {{ log('this is mocked_child_relation: ' ~ mocked_child_relation, True) }}
      {% set ns.node_query = ns.node_query | replace(child_relation, mocked_child_relation) %}

      {% do dbt_flow.build_graph_views(child_node, config) %}
    {% endfor %}
    {% do run_query(create_view_as(relation, ns.node_query)) %}
  {% endif %}
  {% do return(relation) %}
{% endmacro %}

{% macro build_expectations_view(expectations, node_to_test, config) %}
  {% set relation = api.Relation.create(
    database=node_to_test.database,
    schema=node_to_test.schema,
    identifier=dbt_flow.build_flow_identifier(config.test_name, 'expectations_' ~ node_to_test.name)
  ) %}
  {% do run_query(create_view_as(relation, expectations.input_values)) %}
  {% set q %}
    select * from {{ relation }}
  {% endset %}
  {% do return(relation) %}
{% endmacro %}

{% macro ref_tested_model(model_name) %}
  {% set ref_tested_model %}
    -- We add an (unused) reference to the tested model,
    -- so that DBT includes the model as a dependency of the test in the DAG
    select * from {{ ref(model_name) }}
  {% endset %}
{% endmacro %}

