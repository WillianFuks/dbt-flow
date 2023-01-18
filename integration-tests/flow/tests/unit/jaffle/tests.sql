-- depends_on: {{ ref('stg__customers') }}
-- depends_on: {{ ref('stg__orders') }}
-- depends_on: {{ ref('stg__payments') }}

{{
    config(
        tags=['flow-test']
    )
}}

{% call dbt_flow.test('customers', 'test_customers_1', 'test regular dbt flow') %}

  {% call dbt_flow.mock_ref('stg__customers') %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name
  {% endcall %}

  {% call dbt_flow.mock_ref('stg__orders') %}
    select 1 as order_id, 1 as customer_id, '2023-01-02'::Timestamp as order_date, 'statusB' as status
  {% endcall %}

  {% call dbt_flow.mock_ref('stg__payments') %}
    select 1 as payment_id, 1 as order_id, 'methodB' as payment_method, 100 as amount
  {% endcall %}

  {% call dbt_flow.expect() %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name, '2023-01-01'::Timestamp as first_order_date, '2023-01-01'::Timestamp as last_order_date, 1 as number_of_orders, 1.5 as total_amount
  {% endcall %}
{% endcall %}
