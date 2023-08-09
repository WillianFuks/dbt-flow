with customers as (
    select * from {{ ref('stg__customers') }}
),

orders as (
    select * from {{ ref('stg__orders') }}
),

payments as (
    select * from {{ ref('int_payments_mapped') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(order_id) as number_of_orders
    from orders
    group by customer_id
),

customer_payments as (
    select
        orders.customer_id,
        payment_method_name,
        sum(amount) as total_amount
    from payments
    inner join orders on
        payments.order_id = orders.order_id
    group by orders.customer_id, payment_method_name
),

final as (
    select
        customers.customer_id,
        first_name,
        last_name,
        first_order_date,
        last_order_date,
        number_of_orders,
        payment_method_name,
        total_amount
    from customers
    inner join customer_orders
        on customers.customer_id = customer_orders.customer_id
    inner join customer_payments
        on customers.customer_id = customer_payments.customer_id
)


select * from final
