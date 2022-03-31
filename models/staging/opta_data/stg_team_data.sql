with team_data as (

    select * from {{ source('opta_stats', 'team_data') }}

)

select * from team_data