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


{%- set test1 = dbt_flow.setup_test(target_model='customers', test_name='cust1', options={"drop_tables": true}, test_description='simple flow test on customers node, without column "payment_method_name".',
  mocks={
    "stg__orders": """
                   select 1 as order_id, 2 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusB' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 1 as order_id, 1 as payment_method_id, 100 as amount
                     """,
    "test_source_customers.test_customers": """
                                            select 2 as customer_id, 'first_name' as first_name, 'last_name' as last_name
                                            """,
    "payments_mapping": payments_mapping_seed_mock
  },
  expected="""
    select
      2 as customer_id,
      'first_name' as first_name,
      'last_name' as last_name,
      TIMESTAMP('2023-01-01') as first_order_date,
      TIMESTAMP('2023-01-01') as last_order_date,
      1 as number_of_orders,
      100 as total_amount
  """
) -%}


{%- set test2 = dbt_flow.setup_test(target_model='customers', test_name='cust2', options={"drop_tables": false}, test_description="same test as before but with more customers and orders and all columns selected.",
  mocks={
    "stg__orders": """
                   select 1 as order_id, 1 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusA' as status union all
                   select 2 as order_id, 1 as customer_id, TIMESTAMP('2023-01-02') as order_date, 'statusB' as status union all
                   select 3 as order_id, 2 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusB' as status union all
                   select 4 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusB' as status union all
                   select 5 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusA' as status union all
                   select 6 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusA' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 1 as order_id, 2 as payment_method_id, 100 as amount union all
                     select 2 as payment_id, 2 as order_id, 2 as payment_method_id, 102 as amount union all
                     select 3 as payment_id, 3 as order_id, 2 as payment_method_id, 104 as amount union all
                     select 4 as payment_id, 5 as order_id, 1 as payment_method_id, 106 as amount union all
                     select 5 as payment_id, 6 as order_id, 1 as payment_method_id, 108 as amount
                     """,
    "test_source_customers.test_customers": """
                                            select 2 as customer_id, 'first_name2' as first_name, 'last_name2' as last_name
                                            """,
    "payments_mapping": payments_mapping_seed_mock
  },

  expected="""
           select
             1 as customer_id,
             'nameA' as first_name,
             'lastA' as last_name,
             TIMESTAMP('2023-01-01') as first_order_date,
             TIMESTAMP('2023-01-03') as last_order_date, 
             5 as number_of_orders,
             'payment_method_A' as payment_method_name,
             214 as total_amount union all
           select
             1 as customer_id,
             'nameA' as first_name,
             'lastA' as last_name,
             TIMESTAMP('2023-01-01') as first_order_date,
             TIMESTAMP('2023-01-03') as last_order_date, 
             5 as number_of_orders,
             'payment_method_B' as payment_method_name,
             202 as total_amount union all
           select
             2 as customer_id,
             'first_name2' as first_name,
             'last_name2' as last_name,
             TIMESTAMP('2023-01-01') as first_order_date,
             TIMESTAMP('2023-01-01') as last_order_date, 
             1 as number_of_orders,
             'payment_method_B' as payment_method_name,
             104 as total_amount
           """,
) -%}


{%- set test3 = dbt_flow.setup_test(target_model='metrics_customers', test_name='metrics1', options={"drop_tables": false}, test_description="Test if metrics nodes are working correctly.",
  mocks={
    "stg__orders": """
                   select 1 as order_id, 1 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusA' as status union all
                   select 2 as order_id, 1 as customer_id, TIMESTAMP('2023-01-02') as order_date, 'statusB' as status union all
                   select 3 as order_id, 2 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'statusB' as status union all
                   select 4 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusB' as status union all
                   select 5 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusA' as status union all
                   select 6 as order_id, 1 as customer_id, TIMESTAMP('2023-01-03') as order_date, 'statusA' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 1 as order_id, 2 as payment_method_id, 100 as amount union all
                     select 2 as payment_id, 2 as order_id, 2 as payment_method_id, 102 as amount union all
                     select 3 as payment_id, 3 as order_id, 2 as payment_method_id, 104 as amount union all
                     select 4 as payment_id, 5 as order_id, 1 as payment_method_id, 106 as amount union all
                     select 5 as payment_id, 6 as order_id, 1 as payment_method_id, 108 as amount
                     """,
    "test_source_customers.test_customers": """
                                            select 2 as customer_id, 'first_name2' as first_name, 'last_name2' as last_name
                                            """,
    "payments_mapping": payments_mapping_seed_mock
  },
  expected="""
           select
             DATE('2023-01-01') as date_day,
             'payment_method_A' as payment_method_name,
             214 as average_order_amount,
             214 as total_order_amount,
             1.0 as derived_test union all
           select
             DATE('2023-01-01') as date_day,
             'payment_method_B' as payment_method_name,
             153 as average_order_amount,
             306 as total_order_amount,
             0.5 as derived_test
           """
) -%}


{%- set test4 = dbt_flow.setup_test(target_model='orders', test_name='orders1', options={"drop_tables": false}, test_description="test if orders node is working correctly before testing incrementality.",
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


{%- set test5 = dbt_flow.setup_test(target_model='orders', test_name='orders1', options={"drop_tables": false, "test_incremental": true}, test_description="Test default incremental strategy. For Bigquery default is merge.",
  mocks={
    "stg__orders": """
                   select 2 as order_id, 1 as customer_id, TIMESTAMP('2023-01-02') as order_date, 'statusA' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 2 as order_id, 1 as payment_method_id, 102 as amount
                     """,
    "payments_mapping": payments_mapping_seed_mock,
    "orders": """
              select
                1 as order_id,
                1 as customer_id,
                TIMESTAMP('2023-01-01') as order_date,
                'statusA' as status,
                'payment_method_A' as payment_method_name,
                100 as amount
    """
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
             2 as order_id,
             1 as customer_id,
             TIMESTAMP('2023-01-02') as order_date,
             'statusA' as status,
             'payment_method_A' as payment_method_name,
             102 as amount
           """
) -%}


{%- set test6 = dbt_flow.setup_test(target_model='orders2', test_name='orders2', options={"drop_tables": false, "test_incremental": true}, test_description="Test again merge behavior but with unique_key defined.",
  mocks={
    "stg__orders": """
                   select 1 as order_id, 1 as customer_id, TIMESTAMP('2023-01-01') as order_date, 'wrong status' as status union all
                   select 2 as order_id, 1 as customer_id, TIMESTAMP('2023-01-02') as order_date, 'statusA' as status
                   """,
    "stg__payments": """
                     select 1 as payment_id, 1 as order_id, 1 as payment_method_id, 100 as amount union all
                     select 1 as payment_id, 2 as order_id, 1 as payment_method_id, 102 as amount
                     """,
    "payments_mapping": payments_mapping_seed_mock,
    "orders2": """
               select
                 1 as order_id,
                 1 as customer_id,
                 TIMESTAMP('2023-01-01') as order_date,
                 'statusA' as status,
                 'payment_method_A' as payment_method_name,
                 100 as amount union all
               select
                 2 as order_id,
                 1 as customer_id,
                 TIMESTAMP('2023-01-01') as order_date,
                 'statusB' as status,
                 'payment_method_A' as payment_method_name,
                 100 as amount
    """
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
             2 as order_id,
             1 as customer_id,
             TIMESTAMP('2023-01-02') as order_date,
             'statusA' as status,
             'payment_method_A' as payment_method_name,
             102 as amount
           """
) -%}


{%- set test7 = dbt_flow.setup_test(target_model='orders4', test_name='orders7', options={"drop_tables": false}, test_description="test array columns support in bigquery.",
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
             ARRAY(SELECT STRUCT(
               'statusA' as status,
               'payment_method_A' as payment_method_name,
               100 as amount
             )) AS data,
             STRUCT(
               'statusA' as status,
               'payment_method_A' as payment_method_name,
               100 as amount
             ) AS struct_data
          union all
           select
             3 as order_id,
             2 as customer_id,
             TIMESTAMP('2023-01-01') as order_date,
             ARRAY(SELECT STRUCT(
               'statusB' as status,
               'payment_method_B' as payment_method_name,
               104 as amount
             )) as data,
             STRUCT(
               'statusB' as status,
               'payment_method_B' as payment_method_name,
               104 as amount
             ) AS struct_data
          union all
           select
             2 as order_id,
             1 as customer_id,
             TIMESTAMP('2023-01-02') as order_date,
             ARRAY(SELECT STRUCT(
               'statusB' as status,
               'payment_method_A' as payment_method_name,
               102 as amount
             )) AS data,
             STRUCT(
               'statusB' as status,
               'payment_method_A' as payment_method_name,
               102 as amount
             ) AS struct_data
           """
) -%}

{{ dbt_flow.run_tests([test1, test2, test3, test4, test5, test6, test7], global_options={"drop_tables": false, "verbose": true}) }}
