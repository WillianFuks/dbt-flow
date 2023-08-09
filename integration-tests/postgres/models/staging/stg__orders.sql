with A as (

  select 1 as order_id, 1 as customer_id, '2023-01-01'::Timestamp as order_date, 'statusA' as status union all
  select 2 as order_id, 1 as customer_id, '2023-01-02'::Timestamp as order_date, 'statusB' as status

)

SELECT * FROM A
