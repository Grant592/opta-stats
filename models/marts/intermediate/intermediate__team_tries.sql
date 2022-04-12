{#
1. Try count per team per game
2. Try increase - rate in which tries are scored per game
3. Normalised gain - distance made towards try line normalised by probability of scoring a try
#}

with total_tries as (
    
    select
        team_id,
        fxid,
        count(*) as num_tries,

    from {{ ref('stg_match_data') }} 
    where plid not in (
        select club from {{ ref('stg_teams') }}
    )
    and action = 9

    group by 1,2
),

all_games as (

    select distinct
        team_id,
        fxid

    from {{ ref('stg_match_data') }} 
    where plid not in (
        select club from {{ ref('stg_teams') }}
    )
),

all_games_and_tries as (

    select
        all_games.team_id as team_id,
        all_games.fxid as fxid,
        coalesce(total_tries.num_tries, 0) as num_tries
    
    from all_games
    left join total_tries using (team_id, fxid)

)

select * from all_games_and_tries
