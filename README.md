# dbt-flow

`dbt-flow` is a package that adds integration tests functionality for dbt.

Input sources or tables can be mocked with custom data and the model under test have its results compared against the expected results.

# Installation

Just add the following to your project `packages.yml`:

```yml
packages:
    - git: "https://github.com/WillianFuks/dbt-flow.git"
      revision: master
```

## Compatibility

- Support only for `Postgres` and `BigQuery` are official. It's expected that other databases will work as well but no testing is performed on them. Please consider
opening an issue if you find bugs for your database -- while we can't guarantee support we might adapt the code to work with other adapters as well.

- `dbt_flow` was implemented on top of `dbt-core==1.3.1` and `dbt_utils==1.0.0`

# Documentation

## dbt-flow

This package inspired by the great package [dbt_unit_testing](https://github.com/EqualExperts/dbt-unit-testing) and it was created
aiming to offer new functionalities and a new design which allowed for more types of tests (in specific, testing nodes of type *metric* is now supported).

A new design was implemented which allowed to further tests the whole flow of a graph definition (hence the name) -- but in exchange for that, this package is expected to be
slower as well (as it creates mocked tables for each node in the flow graph -- a design that proved to be required in order to test against BigQuery otherwise we get errors of query complexity due too many CTEs).

Also, keep in mind that this is an alpha implementation and many features available in `dbt_unit_testing` are not supported here, mainly inputs of type **csv** or
support for **missing columns** in mocks.

We recommend using that package in case this current one doesn't meet your needs (you'll find advantages and disavantages on both implementations approaches).

## How It Works

Simply define your code tests in ".sql" files in the `test` folder. Here's an example of a test running for the model *metrics_customers* as defined
in *integration-tests/jaffle-shop/tests/unit/tests.sql*:

```jinja
-- depends_on: {{ ref('stg_customers') }}
-- depends_on: {{ ref('stg_orders') }}
-- depends_on: {{ ref('stg_payments') }}
-- depends_on: {{ ref('dbt_metrics_default_calendar') }}

{{
    config(
        tags=['unit-test']
    )
}}

{% call dbt_flow.test('metrics_customers', 'test_metric_1', 'metrics table should yield expected result') %}

  {% call dbt_flow.mock_ref('stg_customers') %}
      select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name
  {% endcall %}

  {% call dbt_flow.mock_ref('stg_orders') %}
      select 1 as customer_id, 1 as order_id, '2023-01-01'::Timestamp as order_date
  {% endcall %}

  {% call dbt_flow.mock_ref('stg_payments') %}
       select 1 as order_id, 1.5 as amount
  {% endcall %}

  {% call dbt_flow.expect() %}
      select '2023-01-01'::Timestamp as metric_start_date, '2023-01-01'::Timestamp as metric_end_date, 1.5 as average_order_amount, 1.5 as total_order_amount
  {% endcall %}
{% endcall %}
```

And then run tests as the regular test framework:

    dbt test -s tag:unit-test

Notice that, contrary to `dbt_unit_testing`, this package **does not require** `ref` or `source` to be mocked -- use them just as you normally would.

What changes though is that the *test.sql* file requires to have the strings:

    -- depends_on: {{ ref('your model name') }}

for each node being tested into the whole flow. We chose this approach as we didn't want to make changes to the actual queries building the models and writing down
each node dependency in the test file didn't seem a considerable compromise so we opted in for this design.

To begin a test definition, simply call the `dbt_flow.test` macro:

```jinja
{% call dbt_flow.test('metrics_customers', 'test_metric_1', 'metrics table should yield expected result') %}
```

- First argument is the name of the model being tested, in this case it's *metrics_customers* which is a model that relies on metrics nodes.

- Then we have *test_metric_1* that is the name of this test and it'll be used to differentiate the tables created in the database

- Finally last argument is a description of the test, currently not particularly used for anything inside the code.

After calling the test, describe all your input mocks, defined as sql queries:

```jinja
{% call dbt_flow.mock_ref('stg_customers') %}
    select 1 as customer_id, 'first_name' as first_name, 'last_name' as last_name
{% endcall %}
```

This will replace the model *stg_customers* with the defined mock. Sources are also available for mocking, simply use the `dbt_flow.mock_source` macro instead.

The final piece is the expected results:

```jinja
{% call dbt_flow.expect() %}
    select '2023-01-01'::Timestamp as metric_start_date, '2023-01-01'::Timestamp as metric_end_date, 1.5 as average_order_amount, 1.5 as total_order_amount
{% endcall %}
```

If results are not as expected by the end of the test an error will raise.

## Status

`dbt_flow` is in **alpha** status so bugs and backward-incompatible changes may occur. Please do consider opening issues or sending a Pull Request to contribute to this project.
