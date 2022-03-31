with descriptors as (

    select * from {{ source('opta_stats', 'descriptions') }}

)

select * from descriptors