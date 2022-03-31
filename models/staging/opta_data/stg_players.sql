with players as (

    select distinct
        plid,
        plforn,
        plsurn
    
    from {{ source('opta_stats', 'team_data') }}

)

select * from players