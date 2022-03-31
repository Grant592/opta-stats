with teams as (

    select * from {{ source('opta_stats', 'teams') }}

)

select * from teams