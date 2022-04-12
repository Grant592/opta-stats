{{ config(materialized='table') }}

{% set outcomes = dbt_utils.get_column_values(table=ref('intermediate__second_row'), column='outcome_2') %}

with lineout_totals as (

    select 
        plid,
        count(distinct fxid) as num_games,
        count(1) as num_lo_involvements,
        count(case when lower(action_type) like '%win%' then 1 else null end) as count_won,
        count(case when lower(action_type) like '%steal%' then 1 else null end) as count_steal,
        {% for outcome in outcomes %}
        {% if outcome %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', outcome) | lower %}
                round(count(case when outcome_2 = '{{ outcome }}' then 1 else null end) / count(1) * 100, 1) as {{ outcome_label }}{% if not loop.last %},{% endif %}
        {% endif %}
        {% endfor %}


    from {{ ref('intermediate__second_row') }} second_row
    group by 1
),

player_minutes as (

    select
        plid,
        sum(mins) as mins_played
    
    from {{ ref('stg_team_data') }} as team_data

    where exists (

        select 1 
        from {{ ref('stg_team_data') }} as team_data_b
        where team_data.plid = team_data_b.plid
        and team_data_b.posid in (4,5)
    )

    group by 1

),

lineouts_per_min as (

    select
        players.plforn || ' ' || players.plsurn as player_name,
        player_minutes.mins_played as mins_played,
        round(lineout_totals.num_lo_involvements / player_minutes.mins_played * 80, 2) as lo_involvements_per_80,
        round(lineout_totals.count_won / player_minutes.mins_played * 80, 2) as lo_won_per_80,
        round(lineout_totals.count_steal / player_minutes.mins_played * 80, 2) as lo_stolen_per_80,
        lineout_totals.*

    from lineout_totals
    inner join player_minutes using (plid)
    inner join {{ ref('stg_players') }} as players using (plid)

)

select * from lineouts_per_min