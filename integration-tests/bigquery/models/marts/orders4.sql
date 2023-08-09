{{
    config(
        materialized='incremental',
        partition_by={
          "field": "order_date",
          "data_type": "timestamp",
          "granularity": "day"
        }
    )
}}


with orders as (
  select * from {{ ref('stg__orders') }}
),

payments as (
  select * from {{ ref('int_payments_mapped') }}
),

orders_payments as (
  select
    orders.order_id,
    orders.customer_id,
    order_date,
    ARRAY(SELECT STRUCT(
      status,
      payment_method_name,
      amount
    )) AS data,
    STRUCT(
      status,
      payment_method_name,
      amount
    ) AS struct_data
  from payments
  inner join orders on
    payments.order_id = orders.order_id

  {% if is_incremental() %}

    where order_date > (select max(order_date) from {{ this }} )

  {% endif %}
)


select * from orders_payments
