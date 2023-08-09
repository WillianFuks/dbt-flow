with payments as (
    select * from {{ ref('stg__payments') }}
),

payments_mapping as (

  select * from {{ ref('payments_mapping') }}

)

select
  payments.payment_id,
  payments.order_id,
  payments_mapping.name as payment_method_name,
  payments.amount
from payments join payments_mapping
  on payments.payment_method_id = payments_mapping.payment_method_id
