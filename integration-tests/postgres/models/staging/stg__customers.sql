{#/*
  This node data comes from a source whose data resembles the following:

  select 1 as customer_id, 'nameA' as first_name, 'lastA' as last_name union all
  select 2 as customer_id, 'nameB' as first_name, 'lastB' as last_name

*/#}

WITH source_customers AS (

  SELECT
    *
  FROM {{ source('test_source_customers', 'test_customers') }}

),

test_customers AS (

  select 1 as customer_id, 'nameA' as first_name, 'lastA' as last_name

)

SELECT
  *
FROM source_customers
UNION
SELECT
  *
FROM test_customers


{# --this is used when running dbt; simply uncomment this part and comment what's above.
with test_customers AS (

  select 1 as customer_id, 'nameA' as first_name, 'lastA' as last_name

)

select * from test_customers
#}
