{{ config(materialized='table') }}

{% set outcomes = dbt_utils.get_column_values(table=ref('intermediate__scrum_half'), column='action_result') %}

with kick_totals as (

    select
        plid,
        count(distinct fxid) as num_game,
        count(1) as num_kicks,
        round(count(case when landed_in_fifteens is true then 1 else null end) / count(1) * 100, 1) as landed_in_15_rate,

        {% for outcome in outcomes %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', outcome) | lower %}
                round(count(case when action_result = '{{ outcome }}' then 1 else null end) / count(1) * 100, 1) as {{ outcome_label }} {% if not loop.last %},{% endif %}   
        {% endfor %}
        

    from {{ ref('intermediate__scrum_half') }}

    group by 1

),

player_minutes as (

    select
        team_data.plid as plid,
        sum(team_data.mins) as mins_played
    
    from {{ ref('stg_team_data') }} as team_data

    where exists (

        select 1 
        from {{ ref('stg_team_data') }} as team_data_b
        where team_data.plid = team_data_b.plid
        and team_data_b.posid = 9
    )

    group by 1

),

kicks_per_min as (

    select
        players.plforn || ' ' || players.plsurn as player_name,
        player_minutes.mins_played as mins_played,
        round(kick_totals.num_kicks / player_minutes.mins_played * 80, 2) as kicks_per_80,
        kick_totals.*

from kick_totals
inner join player_minutes using (plid)
inner join {{ ref('stg_players') }} as players using (plid)

)

select * from kicks_per_min

