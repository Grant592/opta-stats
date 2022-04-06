with all_match_data as (
    select 
        match_data.fxid as fxid,
        match_data.plid as plid,
        match_data.action as action,
        match_data.actiontype as action_type_num,
        descriptions1.qualifier_descriptor as action_type, -- should really rename these at staging layer and not in these models
        descriptions2.qualifier_descriptor as action_result,
        descriptions3.qualifier_descriptor as outcome_1,
        descriptions4.qualifier_descriptor as outcome_2
    from {{ ref('stg_match_data') }}  match_data
        left join {{ ref('stg_descriptions') }} as descriptions1
        on match_data.actiontype = descriptions1.qualifier
        left join {{ ref('stg_descriptions') }} as descriptions2
        on match_data.actionresult = descriptions2.qualifier
        left join {{ ref('stg_descriptions') }} as descriptions3
        on match_data.qualifier3 = descriptions3.qualifier
        left join {{ ref('stg_descriptions') }} as descriptions4
        on match_data.qualifier4 = descriptions4.qualifier
),

tackles_and_carries as (

  select 
      * 
  from all_match_data
  where action in (1,2)
),

jackals as (

  select
      *
  from all_match_data
  where action_type_num = 276
    
)


select * from tackles_and_carries
union all
select * from jackals

{#

1. Num games
2. Num minutes
3. tackles per minute
4. carries per minute
5. turnovers per minute

#}