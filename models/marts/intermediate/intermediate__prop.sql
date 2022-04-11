{#
Scrummaging metrics

1. Total number of scrums in database
2. Split by offensive and defensive
#}


with all_scrums as (

  select 

      match_data.fxid as fxid,
      match_data.plid as plid,
      match_data.action as action,
      descriptions_a.qualifier_descriptor as action_type,
      descriptions_b.qualifier_descriptor as action_result

  from {{ ref('stg_match_data') }} as match_data

  inner join {{ ref('stg_descriptions') }} as descriptions_a -- Would join later but need string to replace Won/lost
  on match_data.actiontype = descriptions_a.qualifier

  inner join {{ ref('stg_descriptions') }} as descriptions_b
  on match_data.actionresult = descriptions_b.qualifier

  where exists (
    select 1 from {{ ref('stg_team_data') }} as team_data
    where match_data.plid = team_data.plid
    and posid in (1,3)
  )
  and match_data.action in (28,29)

),

corrected_scrums as (
  select
      fxid,
      plid,
      action,
      action_type,
      case
        when action = 29 then regexp_replace(action_result, r'(Won)', 'Lost')
        when action = 29 then regexp_replace(action_result, r'(Lost)', 'Won')
        else action_result 
      end as action_result
  from all_scrums
)

select * from corrected_scrums