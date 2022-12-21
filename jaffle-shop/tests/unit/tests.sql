{{
    config(
        tags=['unit-test', 'no-db-dependency']
    )
}}

{% call dbt_unit_testing.test('metrics_customers', 'metrics table should yield expected result') %}

  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1 as customer_id, 1 as order_id, '2023-01-01'::Timestamp as order_date
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
     select 1 as order_id, 1.5 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select '2023-01-01'::Timestamp as metric_start_date, '2023-01-01'::Timestamp as metric_end_date, 1.5 as average_order_amount, 1.5 as total_order_amount
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'customers table should return expected result') %}

  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1 as customer_id, 1 as order_id, '2023-01-01'::Timestamp as order_date
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
     select 1 as order_id, 1.5 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name, '2023-01-01'::Timestamp as most_recent_order, 1 as number_of_orders, 1.5 as customer_lifetime_value
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should show customer_id without orders') %}

  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, '' as first_name, '' as last_name
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select null::numeric as customer_id, null::numeric as order_id, null as order_date  where false
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
     select null::numeric as order_id, null::numeric as amount where false
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should show customer name') %}

  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select null::Numeric as customer_id, 'John' as first_name, 'Doe' as last_name
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select null::numeric as customer_id, null::numeric as order_id, null as order_date  where false
  {% endcall %}

  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
     select null::numeric as order_id, null::numeric as amount where false
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select null::Numeric as customer_id, 'John' as first_name, 'Doe' as last_name
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should sum order values to calculate customer_lifetime_value') %}
  
  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, '' as first_name, '' as last_name
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1001 as order_id, 1 as customer_id, null as order_date
    UNION ALL
    select 1002 as order_id, 1 as customer_id, null as order_date
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
    select 1001 as order_id, 10 as amount
    UNION ALL
    select 1002 as order_id, 10 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id, 20 as customer_lifetime_value
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should calculate the number of orders') %}
  
  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, '' as first_name, '' as last_name
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1001 as order_id, 1 as customer_id, null as order_date
    UNION ALL
    select 1002 as order_id, 1 as customer_id, null as order_date
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
    select 1001 as order_id, 0 as amount
    UNION ALL
    select 1002 as order_id, 0 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id, 2 as number_of_orders
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should calculate most recent order') %}
  
  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, '' as first_name, '' as last_name
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1001 as order_id, 1 as customer_id, '2020-10-01'::Timestamp as order_date
    UNION ALL
    select 1002 as order_id, 1 as customer_id, '2021-01-02'::Timestamp as order_date
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
    select 1001 as order_id, 0 as amount
    UNION ALL
    select 1002 as order_id, 0 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id, '2021-01-02'::Timestamp as most_recent_order
  {% endcall %}
{% endcall %}

UNION ALL

{% call dbt_unit_testing.test('customers', 'should calculate first order') %}
  
  {% call dbt_unit_testing.mock_ref ('stg_customers') %}
    select 1 as customer_id, '' as first_name, '' as last_name
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_orders') %}
    select 1001 as order_id, 1 as customer_id, '2020-10-01'::Timestamp as order_date
    UNION ALL
    select 1002 as order_id, 1 as customer_id, '2021-01-02'::Timestamp as order_date
  {% endcall %}
  
  {% call dbt_unit_testing.mock_ref ('stg_payments') %}
    select 1001 as order_id, 0 as amount
    UNION ALL
    select 1002 as order_id, 0 as amount
  {% endcall %}

  {% call dbt_unit_testing.expect() %}
    select 1 as customer_id, '2020-10-01'::Timestamp as first_order
  {% endcall %}
{% endcall %}
