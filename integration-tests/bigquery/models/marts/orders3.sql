{# --Equal to `orders.sql` but here we test the default incremental_strategy and an unique_key as input. #}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
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
    status,
    payment_method_name,
    amount
  from payments
  inner join orders on
    payments.order_id = orders.order_id

  {% if is_incremental() %}

    where order_date > (select max(order_date) from {{ this }} )

  {% endif %}
)


select * from orders_payments
