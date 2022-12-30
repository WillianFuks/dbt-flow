{% macro mock_ref(model_name, options={}) %}
    {% set mock = {
        'type': 'mock',
        'resource_type': 'model',
        'name': model_name,
        'options': options,
        'input_values': caller()}
    %} 
  {% do return(dbt_flow.append_json(mock)) %}
{% endmacro %}

{% macro mock_source(source_name, table_name, options={}) %}
    {% if not table_name %}
        {{ dbt_flow.raise_error('Table name must be provided for source') }}
    {% endif %}
    {% set mock = {
        'type': 'mock',
        'resource_type': 'source',
        'name': table_name,
        'source_name': source_name,
        'options': options,
        'input_values': caller()}
    %} 
    {% do return(dbt_flow.append_json(mock)) %}
{% endmacro %}

{% macro expect(options={}) %}
    {% set expectations = {
        'type': 'expectations',
        'options': options,
        'input_values': caller()}
    %} 
    {% do return(dbt_flow.append_json(expectations)) %}
{% endmacro %}

{% macro append_json(json) %}
    {% do return(json | tojson() ~ '####_JSON_LINE_DELIMITER_####') %}
{% endmacro %}

{% macro split_json_str(json_str) %}
    {% set lines = json_str.split('####_JSON_LINE_DELIMITER_####') | map('trim') | reject('==', '') | list %}
    {% do return(dbt_flow.map(lines, fromjson)) %}
{% endmacro %}
