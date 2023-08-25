-- depends_on: {{ ref('dbt_metrics_default_calendar') }}
-- depends_on: {{ ref('stg__customers') }}
-- depends_on: {{ ref('stg__orders') }}
-- depends_on: {{ ref('int_payments_mapped') }}
-- depends_on: {{ ref('stg__payments') }}
-- depends_on: {{ ref('payments_mapping') }}

{{
    config(
        tags=['flow-test']
    )
}}

{%- set payments_mapping_seed_mock = """
  select 1 as payment_method_id, 'payment_method_A' as name union all
  select 2 as payment_method_id, 'payment_method_B' as name
  """ -%}


{%- set test1 = dbt_flow.setup_test(target_model='orders', test_name='orders1', options={"drop_tables": false}, test_description="test if orders node is working correctly before testing incrementality.",
  mocks={
    "stg__orders": """
                   select 1 as order_id, 1 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusA' as status union all
                   select 2 as order_id, 1 as customer_id, TIMESTAMP('2023-01-02') as order_date, 'statusB' as status union all
                   select 3 as order_id, 2 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusB' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 1 as order_id, 1 as payment_method_id, 100 as amount union all
                     select 2 as payment_id, 2 as order_id, 1 as payment_method_id, 102 as amount union all
                     select 3 as payment_id, 3 as order_id, 2 as payment_method_id, 104 as amount
                     """,
    "payments_mapping": payments_mapping_seed_mock
  },
  expected="""
           select
             1 as order_id,
             1 as customer_id,
             TIMESTAMP('2023-01-01') as order_date,
             'statusA' as status,
             'payment_method_A' as payment_method_name,
             100 as amount union all
           select
             3 as order_id,
             2 as customer_id,
             TIMESTAMP('2023-01-01') as order_date,
             'statusB' as status,
             'payment_method_B' as payment_method_name,
             104 as amount union all
           select
             2 as order_id,
             1 as customer_id,
             TIMESTAMP('2023-01-02') as order_date,
             'statusB' as status,
             'payment_method_A' as payment_method_name,
             102 as amount
           """
) -%}


{{ dbt_flow.run_tests([test1], global_options={"drop_tables": false, "verbose": true}) }}
