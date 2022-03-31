with box_kicks as (
    select
        match_data.plid as player_id,
        match_data.actiontype as action_type,
        actionresult
    
    from {{ ref('stg_match_data')}} match_data
    inner join from {{ ref('stg_match_data')}} 
    
    where actiontype = 346
)

select * from box_kicks limit 10