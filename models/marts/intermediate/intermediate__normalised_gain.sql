/* First step - Bin field positions for match data */

with binned_pitch_data as (
    select
        match_data.id,
        match_data.fxid,
        match_data.plid,
        match_data.team_id,
        match_data.x_coord,
        match_data.y_coord,
        match_data.x_coord_end,
        match_data.y_coord_end,
        {{ bin_pitch_markings() }} as pitch_position,
        match_data.metres,
        match_data.playnum,
        match_data.setnum,
        match_data.sequence_id,
        events.event_description,
        descriptions.qualifier_descriptor
    
    from {{ ref('stg_match_data') }} as match_data
    inner join {{ ref('stg_events') }} as events
    on match_data.action = events.action_number
    left join {{ ref('stg_descriptions') }}  as descriptions
    on match_data.actiontype = descriptions.qualifier

    -- where exists (
    --     select 1
    --     from {{ ref('stg_players') }} player_data
    --     where player_data.plid = match_data.plid
    -- )

),

tries_scored as (
    select 
        id,
        fxid,
        team_id,
        pitch_position,
        playnum,
        setnum,
        sequence_id,
        1 as try_scored,
        event_description
    from binned_pitch_data bpd
    where exists (
        select 1
        from binned_pitch_data bpd2
        where bpd.fxid = bpd2.fxid
        and bpd.team_id  = bpd2.team_id
        and bpd.playnum = bpd2.playnum
        and bpd.setnum = bpd2.setnum
        and bpd.sequence_id = bpd2.sequence_id
        and ( 
            bpd2.event_description = 'Try'
            or
            bpd2.qualifier_descriptor = 'Try Scored'
        )
    )
),

tries_not_scored as (
    select 
        id,
        fxid,
        team_id,
        pitch_position,
        playnum,
        setnum,
        sequence_id,
        0 as try_scored,
        event_description
    from binned_pitch_data bpd
    where not exists (
        select 1
        from binned_pitch_data bpd2
        where bpd.fxid = bpd2.fxid
        and bpd.team_id  = bpd2.team_id
        and bpd.playnum = bpd2.playnum
        and bpd.setnum = bpd2.setnum
        and bpd.sequence_id = bpd2.sequence_id
        and ( 
            bpd2.event_description = 'Try'
            or
            bpd2.qualifier_descriptor = 'Try Scored'
        )
    )
),

all_sequences as (

    select * from tries_scored
    union all
    select * from tries_not_scored

),

initial_sequence_events as (

    select * from all_sequences
    qualify row_number() over (partition by fxid, team_id, playnum, setnum, sequence_id order by id) = 1
    
)


select
    team_id,
    pitch_position,
    try_scored,
    count(*) as total_tries,
    round(count(*) / sum(count(*)) over (partition by team_id) * 100, 2) as prob_try_scored,
    round(count(*) / sum(count(*)) over (partition by team_id, try_scored) * 100, 2) as conditional_prob_try_scored

from initial_sequence_events

group by 1,2,3

order by 1



