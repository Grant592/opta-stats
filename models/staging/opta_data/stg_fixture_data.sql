with fixture_data as (

    select * from {{ source('opta_stats', 'fix_data') }}
        
)

select * from fixture_data