with source as (
    
    select * from {{ source('test_source', 'test_table') }}

)

select * from source
