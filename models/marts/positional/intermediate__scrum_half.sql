{#

1. Number of box kicks per game
2. Number that land in 15m channel
3. Percent for each outcome

#}

{{ 
    config(
        materialized='view'
    )
}}

with box_kicks as (

    select
        match_data.plid as plid,
        match_data.fxid as fxid,
        match_data.actiontype as action_type,
        descriptions.qualifier_descriptor as action_result,
        case
          when y_coord <= 15 or y_coord >=55 then true
          else false end
        as landed_in_fifteens

    from {{ ref('stg_match_data') }} as match_data
    inner join {{ ref('stg_descriptions') }} as descriptions
    on match_data.actionresult = descriptions.qualifier

    where match_data.actiontype = 346

)

select * from box_kicks
