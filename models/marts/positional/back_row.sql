{{ config(materialized='table') }}

{% set carry_labels_query %}
select distinct action_type
from {{ ref('intermediate__back_row') }}
where action = 1
{% endset %}

{% set results = run_query(carry_labels_query) %}

{% if execute %}
    {% set carry_labels = results.columns[0].values() %}
{% else %}
    {% set carry_labels = [] %}
{% endif %}


{{ log(carry_labels, info=True) }}

{% set tackle_labels_query %}
select distinct action_type
from {{ ref('intermediate__back_row') }}
where action = 2
{% endset %}

{% set results = run_query(tackle_labels_query) %}

{% if execute %}
    {% set tackle_labels = results.columns[0].values() %}
{% else %}
    {% set tackle_labels = [] %}
{% endif %}

{{ log(tackle_labels, info=True) }}

with carry_totals as (

    select 
        plid,
        count(distinct fxid) as num_games,
        count(case when action = 1 then 1 else null end) as num_carries,
        {% for label in carry_labels %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', label) | lower %}
                round(safe_divide(count(case when action_type = '{{ label }}' then 1 else null end), count(1)) * 100, 1) as {{ outcome_label }}{% if not loop.last %},{% endif %}
        {% endfor %}


    from {{ ref('intermediate__back_row') }} back_row
    where action = 1
    group by 1
),

tackle_totals as (

    select 
        plid,
        count(case when action = 2 then 1 else null end) as num_tackles,
        {% for label in tackle_labels %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', label) | lower %}
                round(safe_divide(count(case when action_type = '{{ label }}' then 1 else null end), count(1)) * 100, 1) as {{ outcome_label }}{% if not loop.last %},{% endif %}
        {% endfor %}


    from {{ ref('intermediate__back_row') }} back_row
    where action = 2
    group by 1
),

jackal_totals as (

    select 
        plid,
        count(case when action_type = 'Jackal' then 1 else null end) as num_jackal_attempts,
        count(case when action_type = 'Jackal' and action_result = 'Success' then 1 else null end) as successful_jackal_attempts,
        count(case when action_type = 'Jackal' and action_result = 'Fail' then 1 else null end) as failed_jackal_attempts,
        coalesce(round(safe_divide(count(case when action_type_num = 276 and action_result = 'Success' then 1 else null end),count(case when action_type_num = 276 and action_result = 'Fail' then 1 else null end)),2), -1) as jackal_success_odds

    from {{ ref('intermediate__back_row') }} back_row
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
        and team_data_b.posid in (6,7,8)

    )

    group by 1

),

summary_per_min as (

    select
        players.plforn || ' ' || players.plsurn as player_name,
        player_minutes.mins_played as mins_played,

        round(carry_totals.num_carries / player_minutes.mins_played * 80, 2) as carries_per_80,
        round(tackle_totals.num_tackles / player_minutes.mins_played * 80, 2) as tackles_per_80,
        round(jackal_totals.num_jackal_attempts / player_minutes.mins_played * 80, 2) as jackal_attempts_per_80,

        carry_totals.num_carries as num_carries,
        {% for label in carry_labels %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', label) | lower %}
            carry_totals.{{ outcome_label }} as {{ outcome_label }},
        {% endfor %}

        tackle_totals.num_tackles as num_tackles,
         {% for label in tackle_labels %}
            {% set outcome_label = modules.re.sub('[ \-()]', '_', label) | lower %}
            tackle_totals.{{ outcome_label }} as {{ outcome_label }},
        {% endfor %}

        jackal_totals.num_jackal_attempts as num_jackal_attempts,
        jackal_totals.successful_jackal_attempts as successful_jackal_attempts,
        jackal_totals.failed_jackal_attempts as failed_jackal_attempts,
        jackal_totals.jackal_success_odds as jackal_success_odds


    from carry_totals
    inner join player_minutes using (plid)
    inner join tackle_totals using (plid)
    inner join jackal_totals using (plid)
    inner join {{ ref('stg_players') }} as players using (plid)

)

select * from summary_per_min