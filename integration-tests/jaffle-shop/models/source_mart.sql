with data as (

    select * from {{ ref('stg_source_model') }}

)

select * from data
