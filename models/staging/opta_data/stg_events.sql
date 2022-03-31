with events as (

    select
        action_ as action_number,
        event_ as event_description
    
    from {{ source('opta_stats', 'events_') }}

)

select * from events