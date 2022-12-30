{% macro test(model_name, test_name, test_description, options={}) %}
    {% if execute %}
        {% if not adapter.check_schema_exists(database=target.database, schema=target.schema) %}
            {% do adapter.create_schema(api.Relation.create(database=target.database, schema=target.schema)) %}
        {% endif %}
  
        {% set test_configuration = {
          'model_name': model_name,
          'test_name': test_name,
          'description': test_description,
          'options': dbt_flow.merge_configs([options])}
        %}
  
        {% set mocks_and_expectations_json_str = caller() %}
  
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
        {% do mock_or_expectation.update(
            {'options': dbt_flow.merge_configs([test_configuration.options, mock_or_expectation.options]),
             'input_values': mock_or_expectation.input_values
            })
        %}
    {% endfor %}

    {% set mocks = mocks_and_expectations | selectattr('type', '==', 'mock') | list %}
    {% set expectations = mocks_and_expectations | selectattr('type', '==', 'expectations') | first %}

    {% for mock in mocks %}
      {% do mock.update({'unique_id': dbt_flow.graph_node(mock.source_name, mock.name).unique_id}) %}
    {% endfor %}

    {% set mocks_and_expectations_json = {
      'mocks': mocks,
      'expectations': expectations
      }
    %}

    {% do return(mocks_and_expectations_json) %}
{% endmacro %}

{# source nodes don't have information on them about the appropriate database and schema to where
tables should be built so this macro saves this information in the test configuration and it is used
later on when its mocking is requried #}
{% macro update_adapter_config_info(config, node) %}
    {{ config.update({
      'database': node.database,
      'schema': node.schema
    }) }}
{% endmacro %}

{% macro run_test(test_configuration) %}
    {% set expectations = test_configuration.expectations %}
    {% set node = dbt_flow.model_node(test_configuration.model_name) %}
    {% do dbt_flow.update_adapter_config_info(test_configuration, node) %}
    {% set relations, flow_relations = [], [] %}
    {% do dbt_flow.build_graph_views(node, test_configuration, relations, flow_relations) %}
    {% set flow_relation = dbt_flow.build_flow_relation(node.database, node.schema, node.name, test_configuration.test_name) %}
    {% set flow_relation = api.Relation.create(
        database=node.database, schema=node.schema, identifier=dbt_flow.build_flow_identifier(test_configuration.test_name, node.name))
    %}
    {% set expect_rel = dbt_flow.build_expectations_table(expectations, node, test_configuration) %}

    {% set results = run_query(dbt_utils.test_equality(expect_rel, flow_relation)) %}
    {% if results.columns[0].values() %}
        {% do log('\n' * 2 ~ 'Expected results are not equal to the actual ones:\n', True) %}
        {% do log('*' * 90, True) %}
        {% do results.print_table(max_columns=results.columns.keys() | length) %}
        {% do log('\n' ~ '*' * 90 ~ '\n', True) %}
        {% do exceptions.raise_compiler_error('\n\n' ~ 'Flow test for model ' ~ test_configuration.model_name ~ ' failed!!!\n') %}
    {% endif %}
{% endmacro %}

{% macro build_flow_relation(database, schema, name, test_name='') %}
    {% if test_name %}
        {% set identifier = dbt_flow.build_flow_identifier(test_name, name) %}
    {% else %}
        {% set identifier = name %}
    {% endif %}
    {% set flow_relation = api.Relation.create(
        database=database,
        schema=schema, 
        identifier=identifier
    ) %}
    {% do return(flow_relation) %}
{% endmacro %}

{% macro create_flow_table(flow_relation, cache, query) %}
    {% if flow_relation not in cache %}
        {% do run_query(create_table_as(false, flow_relation, query)) %}
    {% endif %}
{% endmacro %}

{% macro build_graph_views(node, config, relations, flow_relations) %}
    {# metric nodes do not generate any query to be run so there's no relation we need to extract#}
    {% if node.resource_type == 'metric' %}
        {% set flow_relation = none %}
    {% else %}
        {% if node.resource_type == 'source' %}
            {# the schema and database defined in the source node can point to outside tables so we make sure to use
               our own database to create the mocked tables #}
            {% set flow_relation = dbt_flow.build_flow_relation(
                config.database, config.schema, 'source_' ~ node.name, config.test_name).render() %}
        {% else %}
            {% set flow_relation = dbt_flow.build_flow_relation(node.database, node.schema, node.name, config.test_name).render() %}
        {% endif %}
    {% endif %}

    {# relation is already processed #}
    {% if flow_relation in flow_relations %}
        {% do return((relations, flow_relations)) %}
    {% endif %}
 
    {% set mock = config.mocks | selectattr('unique_id', '==', node.unique_id) | first %}
    {% if mock %}
        {% do dbt_flow.create_flow_table(flow_relation, flow_relations, mock.input_values) %}
    {% else %}
        {% for node_id in node.depends_on.nodes %}
            {% set child_node = dbt_flow.node_by_id(node_id) %}
            {# metric nodes are different from the rest. We need to further dig their
               dependencies in order to find their references #}
            {% if child_node.resource_type == 'metric' %}
                {% for metric_deps_node_id in child_node.depends_on.nodes %}
                    {% set metric_child_node = dbt_flow.node_by_id(metric_deps_node_id) %}
                    {% set child_relations, child_flow_relations = dbt_flow.build_graph_views(metric_child_node, config, relations, flow_relations) %}
                    {% for i in range(child_relations | length) %}
                        {{ relations.append(child_relations[i]) }}
                        {{ flow_relations.append(child_flow_relations[i]) }}
                    {% endfor %}
                {% endfor %}
            {% else %}
                {% set child_relations, child_flow_relations = dbt_flow.build_graph_views(child_node, config, relations, flow_relations) %}
                {% for i in range(child_relations | length) %}
                    {{ relations.append(child_relations[i]) }}
                    {{ flow_relations.append(child_flow_relations[i]) }}
                {% endfor %}
            {% endif %}
        {% endfor %}

        {% if node.resource_type != 'metric' %}
            {% set ns = namespace(node_query = render(node.raw_code)) %}
            {% set relations = relations | unique | list %}
            {% set flow_relations = flow_relations | unique | list %}

            {% for i in range(relations | length) %}
                {% set ns.node_query = ns.node_query | replace(relations[i], flow_relations[i]) %}
            {% endfor %}
            {% do dbt_flow.create_flow_table(flow_relation, flow_relations, ns.node_query) %}
        {% endif %}
    {% endif %}

    {% if node.resource_type != 'metric' %}
        {# source nodes already have their relation properly defined in the node definition #}
        {% if node.resource_type == 'source' %}
            {% set relation = node.relation_name %}
        {% else %}
            {% set relation = dbt_flow.build_flow_relation(node.database, node.schema, node.name).render() %}
        {% endif %}
        {{ relations.append(relation) }}
        {{ flow_relations.append(flow_relation) }}
    {% endif %}

    {% set relations = relations | unique | list %}
    {% set flow_relations = flow_relations | unique | list %}
    {% do return((relations, flow_relations)) %}
{% endmacro %}

{% macro build_expectations_table(expectations, node_to_test, config) %}
    {% set relation = api.Relation.create(
        database=node_to_test.database,
        schema=node_to_test.schema,
        identifier=dbt_flow.build_flow_identifier(config.test_name, 'expectations_' ~ node_to_test.name)
    ) %}
    {% do run_query(create_table_as(false, relation, expectations.input_values)) %}
    {% do return(relation) %}
{% endmacro %}
