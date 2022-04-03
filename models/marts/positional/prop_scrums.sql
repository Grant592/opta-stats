{{ config(materialized='table') }}

{% set outcomes = dbt_utils.get_column_values(table=ref('intermediate__prop'), column='action_result') %}

with scrum_summary as (

  select
      players.plforn || ' ' || players.plsurn as player_name,
      case 
        when action = 28 then 'Offensive Scrum'
        when action = 29 then 'Defensive Scrum'
      end as action,
      count(*) as number_of_scrums,

      {% for outcome in outcomes %}
        {% set outcome_label = modules.re.sub('[ \-()]', '_', outcome) | lower %}
            count(case when action_result = '{{ outcome }}' then 1 else null end) as num_{{ outcome_label }},
            round(count(case when action_result = '{{ outcome }}' then 1 else null end)/count(*) * 100, 1) as {{ outcome_label }}_rate{% if not loop.last %},{% endif %}
      {% endfor %}

  from {{ ref('intermediate__prop') }} as prop
  inner join {{ ref('stg_players') }} as players
  on prop.plid = players.plid

  group by 1,2
  order by won_penalty_rate desc
)

select * from scrum_summary