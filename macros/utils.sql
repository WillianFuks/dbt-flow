{% macro build_flow_identifier(test_name, identifier) %}
    {% do return('dbt_flow_' ~ test_name ~ '__' ~ identifier) %}
{% endmacro %}

{% macro map(items, f) %}
    {% set mapped_items=[] %}
    {% for item in items %}
        {% do mapped_items.append(f(item)) %}
    {% endfor %}
    {% do return(mapped_items) %}
{% endmacro %}

{% macro node_by_id(node_id) %}]
    {% do return(graph.nodes[node_id] if node_id in graph.nodes else
              graph.sources[node_id] if node_id in graph.sources else
              graph.metrics[node_id]) %}
{% endmacro %}

{% macro graph_node_by_prefix(prefix, name) %}
    {% do return(graph.nodes[prefix ~ '.' ~ model.package_name ~ '.' ~ name]) %}
{% endmacro %}

{% macro model_node(model_name) %}
    {% set node = nil
        | default(dbt_flow.graph_node_by_prefix('model', model_name))
        | default(dbt_flow.graph_node_by_prefix('snapshot', model_name)) 
        | default(dbt_flow.graph_node_by_prefix('seed', model_name)) %}
    {% if not node %}
        {% do dbt_flow.raise_error('Node ' ~ model.package_name ~ '.' ~ model_name ~ ' not found.') %}
    {% endif %}
    {{ return(node) }}
{% endmacro %}

{% macro source_node(source_name, model_name) %}
    {% do return(graph.sources['source.' ~ model.package_name ~ '.' ~ source_name ~ '.' ~ model_name]) %}
{% endmacro %}

{% macro graph_node(source_name, model_name) %}
    {% if source_name %}
        {% do return(dbt_flow.source_node(source_name, model_name)) %}
    {% else %}
        {% do return(dbt_flow.model_node(model_name)) %}
    {% endif %}
{% endmacro %}

{% macro merge_jsons(jsons) %}
    {% set json = {} %}
    {% for j in jsons %}
        {% for k,v in j.items() %}
            {% do json.update({k: v}) %}
        {% endfor %}
    {% endfor %}
    {% do return(json) %}
{% endmacro %}

{% macro get_config(config_name, default_value) %}
    {% set flow_config = var('flow_config', {}) %}
    {% do return(flow_config.get(config_name, default_value)) %}
{% endmacro %}

{% macro merge_configs(configs) %}
    {% set flow_config = var('flow_config', {}) %}
    {% do return (dbt_flow.merge_jsons([flow_config] + configs)) %}
{% endmacro %}

{% macro raise_error(error_message) %}
    {% do exceptions.raise_compiler_error('\x1b[31m' ~ error_message ~ '\x1b[0m') %}
{% endmacro %}
