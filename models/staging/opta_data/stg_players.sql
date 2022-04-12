with players as (

    select distinct
        plid,
        plforn,
        plsurn
    
    from {{ source('opta_stats', 'team_data') }}

    qualify row_number() over(partition by plid) = 1

)

select * from players