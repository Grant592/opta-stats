with match_data as (

    select * from {{ source('opta_stats', 'match_data') }}

)

select * from match_data