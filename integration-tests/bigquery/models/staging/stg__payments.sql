WITH data AS (
  select 1 as payment_id, 1 as order_id, 1 as payment_method_id, 100 as amount union all
  select 2 as payment_id, 2 as order_id, 2 as payment_method_id, 120 as amount union all
  select 2 as payment_id, 2 as order_id, 3 as payment_method_id, 120 as amount
)

SELECT
  *
FROM data
